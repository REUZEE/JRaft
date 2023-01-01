package org.reuze.jraft.service;

import com.baidu.brpc.client.RpcCallback;
import org.reuze.jraft.proto.RaftProto;

/**
 * @author Reuze
 * @Date 01/01/2023
 */
public interface RaftConsensusServiceAsync extends RaftConsensusService {

    RaftProto.VoteResponse preVote(RaftProto.VoteRequest request, RpcCallback<RaftProto.VoteResponse> callback);

    RaftProto.VoteResponse vote(RaftProto.VoteRequest request, RpcCallback<RaftProto.VoteResponse> callback);

    RaftProto.AppendEntriesResponse appendEntries(RaftProto.AppendEntriesRequest request, RpcCallback<RaftProto.AppendEntriesResponse> callback);
}
