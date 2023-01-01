package org.reuze.jraft.service;

import com.baidu.brpc.client.RpcCallback;
import org.reuze.jraft.RaftNode;
import org.reuze.jraft.proto.RaftProto;

/**
 * @author Reuze
 * @Date 31/12/2022
 */
public interface RaftConsensusService {

    RaftProto.VoteResponse preVote(RaftProto.VoteRequest request);

    RaftProto.VoteResponse vote(RaftProto.VoteRequest request);

    RaftProto.AppendEntriesResponse appendEntries(RaftProto.AppendEntriesRequest request);
}
