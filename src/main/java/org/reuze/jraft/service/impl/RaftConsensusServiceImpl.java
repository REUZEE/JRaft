package org.reuze.jraft.service.impl;

import com.baidu.brpc.client.RpcCallback;
import lombok.extern.slf4j.Slf4j;
import org.reuze.jraft.RaftNode;
import org.reuze.jraft.proto.RaftProto;
import org.reuze.jraft.service.RaftConsensusService;

/**
 * @author Reuze
 * @Date 31/12/2022
 */
@Slf4j
public class RaftConsensusServiceImpl implements RaftConsensusService {

    private RaftNode raftNode;

    public RaftConsensusServiceImpl(RaftNode raftNode) {
        this.raftNode = raftNode;
    }

    @Override
    public RaftProto.VoteResponse preVote(RaftProto.VoteRequest request) {
        raftNode.getLock().lock();
        try {
            RaftProto.VoteResponse.Builder builder = RaftProto.VoteResponse.newBuilder();
            builder.setVoteGranted(false);
            builder.setTerm(raftNode.getCurrentTerm());

            if (request.getTerm() < raftNode.getCurrentTerm()) {
                log.info("Receive preVote request from server {} in term {}, my term is {}, granted = {}",
                        request.getCandidateId(), request.getTerm(), raftNode.getCurrentTerm(), builder.getVoteGranted());
                return builder.build();
            }

            boolean isLogOk = request.getLastLogTerm() > raftNode.getLastLogTerm() ||
                    (request.getLastLogTerm() == raftNode.getLastLogTerm() &&
                     request.getLastLogIndex() >= raftNode.getRaftLog().getLastLogIndex());
            if (!isLogOk) {
                log.info("Receive preVote request from server {} in term {}, my term is {}, granted = {}",
                        request.getCandidateId(), request.getTerm(), raftNode.getCurrentTerm(), builder.getVoteGranted());
                return builder.build();
            } else {
                builder.setVoteGranted(true);
            }
            log.info("Receive preVote request from server {} in term {}, my term is {}, granted = {}",
                    request.getCandidateId(), request.getTerm(), raftNode.getCurrentTerm(), builder.getVoteGranted());
            return builder.build();

        } finally {
            raftNode.getLock().unlock();
        }
    }

    @Override
    public RaftProto.VoteResponse vote(RaftProto.VoteRequest request) {
        raftNode.getLock().lock();
        RaftProto.VoteResponse.Builder builder = RaftProto.VoteResponse.newBuilder();
        try {
            builder.setVoteGranted(false)
                    .setTerm(raftNode.getCurrentTerm());
            if (request.getTerm() < raftNode.getCurrentTerm()) {
                log.info("Receive vote request from server {} in term {}, my term is {}, granted = {}",
                        request.getCandidateId(), request.getTerm(), raftNode.getCurrentTerm(), builder.getVoteGranted());
                return builder.build();
            }
            if (request.getTerm() > raftNode.getCurrentTerm()) {
                raftNode.stepDown(request.getTerm());
            }
            boolean isLogOk = request.getLastLogTerm() > raftNode.getLastLogTerm() ||
                    (request.getLastLogTerm() == raftNode.getLastLogTerm() &&
                            request.getLastLogIndex() >= raftNode.getRaftLog().getLastLogIndex());
            if (!isLogOk || raftNode.getVoteFor() != 0) {
                log.info("Receive vote request from server {} in term {}, my term is {}, granted = {}",
                        request.getCandidateId(), request.getTerm(), raftNode.getCurrentTerm(), builder.getVoteGranted());
                return builder.build();
            } else {
                raftNode.setVoteFor(request.getCandidateId());
                raftNode.getRaftLog().updateMetaData(raftNode.getCurrentTerm(), raftNode.getVoteFor(), null, null);
                builder.setVoteGranted(true);
            }
            log.info("Receive vote request from server {} in term {}, my term is {}, granted = {}",
                    request.getCandidateId(), request.getTerm(), raftNode.getCurrentTerm(), builder.getVoteGranted());
            return builder.build();
        } finally {
            raftNode.getLock().unlock();
        }
    }

    @Override
    public RaftProto.AppendEntriesResponse appendEntries(RaftProto.AppendEntriesRequest request) {
        raftNode.getLock().lock();
        try {
            RaftProto.AppendEntriesResponse.Builder builder = RaftProto.AppendEntriesResponse.newBuilder();
            builder.setTerm(raftNode.getCurrentTerm());
            builder.setSuccess(RaftProto.ResCode.RES_CODE_FAIL);
            builder.setLastLogIndex(raftNode.getRaftLog().getLastLogIndex());
            if (request.getTerm() < raftNode.getCurrentTerm()) {
                return builder.build();
            }

            raftNode.stepDown(request.getTerm());
            if (raftNode.getLeaderId() == 0) {
                raftNode.setLeaderId(request.getLeaderId());
                log.info("New leader = {}", raftNode.getLeaderId());
            }
            if (raftNode.getLeaderId() != request.getLeaderId()) {
                log.warn("Peer = {} declares that it's the leader at term = {} which was occupied by leader = {}",
                        raftNode.getLocalServer().getServerId(), request.getTerm(), raftNode.getLeaderId());
                raftNode.stepDown(request.getTerm() + 1);
                builder.setTerm(request.getTerm() + 1);
                return builder.build();
            }
            if (request.getPrevLogIndex() > raftNode.getRaftLog().getLastLogIndex()) {
                log.info("AppendEntries would leave gap, request prevLogIndex = {}, lastLogIndex = {}",
                        request.getPrevLogIndex(), raftNode.getRaftLog().getLastLogIndex());
                return builder.build();
            }
            if (request.getPrevLogIndex() >= raftNode.getRaftLog().getFirstLogIndex()
            && raftNode.getRaftLog().getEntryTerm(request.getPrevLogIndex()) != request.getPrevLogTerm()) {
                log.info("AppendEntries terms don't agree, request prevLogTerm = {}, preLogTerm = {}",
                        request.getPrevLogTerm(), raftNode.getRaftLog().getEntryTerm(request.getPrevLogIndex()));
                return builder.build();
            }

            builder.setSuccess(RaftProto.ResCode.RES_CODE_SUCCESS);
            if (request.getEntriesCount() == 0) {
                log.info("Receive heartbeat from server {} at term {}",
                        request.getLeaderId(), request.getTerm());
                return builder.build();
            }
        } finally {
            raftNode.getLock().unlock();
        }

        return null;
    }
}
