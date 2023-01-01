package org.reuze.jraft;

import com.baidu.brpc.client.BrpcProxy;
import com.baidu.brpc.client.RpcClient;
import com.baidu.brpc.client.instance.Endpoint;
import lombok.Getter;
import lombok.Setter;
import org.reuze.jraft.proto.RaftProto;
import org.reuze.jraft.service.RaftConsensusService;
import org.reuze.jraft.service.RaftConsensusServiceAsync;

/**
 * @author Reuze
 * @Date 30/12/2022
 */
@Getter
@Setter
public class Peer {

    private RpcClient rpcClient;

    private RaftProto.Server server;
    private volatile Boolean voteGranted;
    private RaftConsensusServiceAsync raftConsensusServiceAsync;

    public Peer(RaftProto.Server server) {
        this.server = server;
        this.rpcClient = new RpcClient(new Endpoint(
                server.getEndpoint().getHost(),
                server.getEndpoint().getPort()
        ));
        raftConsensusServiceAsync = BrpcProxy.getProxy(rpcClient, RaftConsensusServiceAsync.class);
    }
}
