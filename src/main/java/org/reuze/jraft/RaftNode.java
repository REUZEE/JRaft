package org.reuze.jraft;

import com.baidu.brpc.client.RpcCallback;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.reuze.jraft.proto.RaftProto;
import org.reuze.jraft.storage.SegmentedLog;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Reuze
 * @Date 30/12/2022
 */
@Slf4j
@Getter
@Setter
public class RaftNode {

    public enum NodeState {
        STATE_FOLLOWER,
        STATE_PRE_CANDIDATE,
        STATE_CANDIDATE,
        STATE_LEADER
    }

    private NodeState state = NodeState.STATE_FOLLOWER;
    private SegmentedLog raftLog;
    private long currentTerm;
    private int leaderId;
    private int voteFor;
    private int commitIndex;

    private Lock lock = new ReentrantLock();

    private RaftOptions raftOptions;
    private ConcurrentMap<Integer, Peer> peerMap = new ConcurrentHashMap<Integer, Peer>();
    private RaftProto.Server localServer;
    private RaftProto.Configuration configuration;

    private ExecutorService executorService;
    private ScheduledExecutorService scheduledExecutorService;
    private ScheduledFuture electionScheduledFuture;
    private ScheduledFuture heartbeatScheduledFuture;

    public RaftNode(RaftOptions raftOptions, List<RaftProto.Server> servers,
                    RaftProto.Server localServer) {
        this.raftLog = new SegmentedLog();
        this.raftOptions = raftOptions;
        this.localServer = localServer;
        RaftProto.Configuration.Builder builder = RaftProto.Configuration.newBuilder();
        for (RaftProto.Server server : servers) {
            builder.addServers(server);
        }
        configuration = builder.build();

    }

    public void init() {
        for (RaftProto.Server server : configuration.getServersList()) {
            if (!peerMap.containsKey(server.getServerId()) && server.getServerId() != localServer.getServerId()) {
                Peer peer = new Peer(server);
                peerMap.put(server.getServerId(), peer);
            }
        }

        executorService = new ThreadPoolExecutor(
                raftOptions.getRaftConsensusThreadNum(),
                raftOptions.getRaftConsensusThreadNum(),
                60,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>()
        );

        scheduledExecutorService = Executors.newScheduledThreadPool(2);
        resetElectionTimer();
    }

    private void resetElectionTimer() {
        if (electionScheduledFuture != null && !electionScheduledFuture.isDone()) {
            electionScheduledFuture.cancel(true);
        }
        electionScheduledFuture = scheduledExecutorService.schedule(() -> startPreVote(),
                getElectionTimeoutMs(), TimeUnit.MILLISECONDS);
    }

    private int getElectionTimeoutMs() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int randomElectionTimeout = raftOptions.getElectionTimeoutMilliseconds() +
                random.nextInt(0, raftOptions.getElectionTimeoutMilliseconds());
        log.info("New election time is after {} ms", randomElectionTimeout);
        return randomElectionTimeout;
    }

    private void startPreVote() {
        lock.lock();
        try {
            log.info("Running preVote in term {}", currentTerm);
            this.state = NodeState.STATE_PRE_CANDIDATE;
        } finally {
            lock.unlock();
        }

        for (RaftProto.Server server : configuration.getServersList()) {
            if (server.getServerId() == this.localServer.getServerId()) {
                continue;
            }
            final Peer peer = peerMap.get(server.getServerId());
            executorService.submit(() -> preVote(peer));
        }
        resetElectionTimer();
    }

    private void preVote(Peer peer) {
        RaftProto.VoteRequest.Builder builder = RaftProto.VoteRequest.newBuilder();
        lock.lock();
        try {
            peer.setVoteGranted(null);
            builder.setCandidateId(localServer.getServerId())
                    .setTerm(currentTerm)
                    .setLastLogTerm(getLastLogTerm())
                    .setLastLogIndex(raftLog.getLastLogIndex());
        } finally {
            lock.unlock();
        }
        RaftProto.VoteRequest request = builder.build();
        log.info("Send preVote request from server {} to server {}",
                localServer.getServerId(), peer.getServer().getServerId());
        peer.getRaftConsensusServiceAsync().preVote(request, new PreVoteResponseCallback(peer, request));
    }

    private class PreVoteResponseCallback implements RpcCallback<RaftProto.VoteResponse> {

        private Peer peer;
        private RaftProto.VoteRequest request;

        public PreVoteResponseCallback(Peer peer, RaftProto.VoteRequest request) {
            this.peer = peer;
            this.request = request;
        }

        @Override
        public void success(RaftProto.VoteResponse voteResponse) {
            lock.lock();
            try {
                peer.setVoteGranted(voteResponse.getVoteGranted());
                if (currentTerm != request.getTerm() || state != NodeState.STATE_PRE_CANDIDATE) {
                    log.info("Receive preVote response from server {}, ignore preVote RPC result",
                            peer.getServer().getServerId());
                    return;
                }
                if (voteResponse.getTerm() > currentTerm) {
                    log.info("Receive preVote response from server {} in term {} bigger than currentTerm {}",
                            peer.getServer().getServerId(), voteResponse.getTerm(), currentTerm);
                    stepDown(voteResponse.getTerm());
                } else {
                    if (voteResponse.getVoteGranted()) {
                        log.info("Receive preVote response from server {} for term {}",
                                peer.getServer().getServerId(), currentTerm);
                        int voteGrantedNum = 1;
                        for (RaftProto.Server server : configuration.getServersList()) {
                            if (server.getServerId() == localServer.getServerId()) {
                                continue;
                            }
                            Peer peer1 = peerMap.get(server.getServerId());
                            if (peer1.getVoteGranted() != null && peer1.getVoteGranted()) {
                                voteGrantedNum++;
                            }
                        }
                        if (voteGrantedNum > configuration.getServersCount() / 2) {
                            log.info("Get majority preVote, voteGrantedNum = {}, start vote", voteGrantedNum);
                            startVote();
                        }
                    } else {
                        log.info("Receive preVote response from server {} but denied", peer.getServer().getServerId());
                    }
                }
            } finally {
                lock.unlock();
            }
        }

        @Override
        public void fail(Throwable throwable) {
            log.warn("Fail to preVote with peer[{}:{}]",
                    peer.getServer().getEndpoint().getHost(),
                    peer.getServer().getEndpoint().getPort());
            peer.setVoteGranted(false);
        }
    }

    private void startVote() {
        lock.lock();
        try {
            currentTerm++;
            log.info("Running for election in term {}",currentTerm);
            state = NodeState.STATE_CANDIDATE;
            leaderId = 0;
            voteFor = localServer.getServerId();
        } finally {
            lock.unlock();
        }
        for (RaftProto.Server server : configuration.getServersList()) {
            if (server.getServerId() == localServer.getServerId()) {
                continue;
            }
            final Peer peer = peerMap.get(server.getServerId());
            executorService.submit(() -> vote(peer));
        }
    }

    public void vote(Peer peer) {
        RaftProto.VoteRequest.Builder builder = RaftProto.VoteRequest.newBuilder();
        lock.lock();
        try {
            peer.setVoteGranted(false);
            builder.setCandidateId(localServer.getServerId())
                    .setTerm(currentTerm)
                    .setLastLogIndex(raftLog.getLastLogIndex())
                    .setLastLogTerm(getLastLogTerm());
        } finally {
            lock.unlock();
        }
        RaftProto.VoteRequest request = builder.build();
        log.info("Send vote request to server {}", peer.getServer().getServerId());
        peer.getRaftConsensusServiceAsync().vote(request, new VoteResponseCallback(peer, request));
    }

    private class VoteResponseCallback implements RpcCallback<RaftProto.VoteResponse> {

        private Peer peer;
        private RaftProto.VoteRequest request;

        public VoteResponseCallback(Peer peer, RaftProto.VoteRequest request) {
            this.peer = peer;
            this.request = request;
        }

        @Override
        public void success(RaftProto.VoteResponse voteResponse) {
            lock.lock();
            try {
                peer.setVoteGranted(voteResponse.getVoteGranted());
                if (currentTerm != request.getTerm() || state != NodeState.STATE_CANDIDATE) {
                    log.info("Receive vote response from server {}, ignore vote result", peer.getServer().getServerId());
                    return;
                }
                if (voteResponse.getTerm() > currentTerm) {
                    log.info("Receive vote response from server {} in term {} bigger than currentTerm {}",
                            peer.getServer().getServerId(), voteResponse.getTerm(), currentTerm);
                    stepDown(voteResponse.getTerm());
                } else {
                    if (voteResponse.getVoteGranted()) {
                        log.info("Receive vote response from server {} for term",
                                peer.getServer().getServerId(), currentTerm);
                        int voteGrantedNum = 0;
                        if (voteFor == localServer.getServerId()) {
                            voteGrantedNum++;
                        }
                        for (RaftProto.Server server : configuration.getServersList()) {
                            if (server.getServerId() == localServer.getServerId()) {
                                continue;
                            }
                            if (peerMap.get(server.getServerId()).getVoteGranted()) {
                                voteGrantedNum++;
                            }
                        }
                        if (voteGrantedNum > configuration.getServersCount() / 2) {
                            log.info("Get majority vote, voteGrantedNum = {}, server {} becomes leader",
                                    voteGrantedNum, localServer.getServerId());
                            becomeLeader();
                        }
                    } else {
                        log.info("Receive vote response from server {} denied", peer.getServer().getServerId());
                    }
                }
            } finally {
                lock.unlock();
            }
        }

        @Override
        public void fail(Throwable throwable) {
            log.warn("Fail to preVote with peer[{}:{}]",
                    peer.getServer().getEndpoint().getHost(),
                    peer.getServer().getEndpoint().getPort());
            peer.setVoteGranted(false);
        }
    }

    public void becomeLeader() {
        state = NodeState.STATE_LEADER;
        leaderId = localServer.getServerId();
        if (electionScheduledFuture != null && !electionScheduledFuture.isDone()) {
            electionScheduledFuture.cancel(true);
        }
        startNewHeartbeat();
    }

    public void startNewHeartbeat() {
        log.info("Send heartbeat to peers = {}", peerMap.keySet());
        for (final Peer peer : peerMap.values()) {
            executorService.submit(() -> appendEntries(peer));
        }
        resetHeartbeatTimer();
    }

    public void resetHeartbeatTimer() {
        if (heartbeatScheduledFuture != null && !heartbeatScheduledFuture.isDone()) {
            heartbeatScheduledFuture.cancel(true);
        }
        heartbeatScheduledFuture = scheduledExecutorService.schedule(() -> startNewHeartbeat(),
                raftOptions.getHeartbeatPeriodMilliseconds(), TimeUnit.MILLISECONDS);
    }

    public void appendEntries(Peer peer) {
        RaftProto.AppendEntriesRequest.Builder builder = RaftProto.AppendEntriesRequest.newBuilder();
        long prevLogIndex = 0;
        long prevLogTerm = 0;

        lock.lock();
        try {
            builder.setLeaderId(localServer.getServerId())
                    .setTerm(currentTerm)
                    .setLeaderCommitIndex(commitIndex)
                    .setPrevLogIndex(prevLogIndex)
                    .setPrevLogTerm(prevLogTerm);

        } finally {
            lock.unlock();
        }

        RaftProto.AppendEntriesRequest request = builder.build();
        RaftProto.AppendEntriesResponse response = peer.getRaftConsensusServiceAsync().appendEntries(request);
    }

    public void stepDown(long newTerm) {
        if (currentTerm > newTerm) {
            return;
        }
        if (currentTerm < newTerm) {
            currentTerm = newTerm;
            leaderId = 0;
            voteFor = 0;
            raftLog.updateMetaData(currentTerm, voteFor, null, null);
        }
        state = NodeState.STATE_FOLLOWER;
        resetElectionTimer();
    }

    public long getLastLogTerm() {
        // todo
        return 1L;
    }
}
