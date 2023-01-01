package org.reuze.jraft;

import com.baidu.brpc.server.RpcServer;
import org.junit.Before;
import org.junit.Test;
import org.reuze.jraft.proto.RaftProto;
import org.reuze.jraft.service.RaftConsensusService;
import org.reuze.jraft.service.impl.RaftConsensusServiceImpl;

import java.util.LinkedList;
import java.util.List;

/**
 * @author Reuze
 * @Date 01/01/2023
 */
public class LeaderElectionTest {

    List<RaftProto.Server> servers = new LinkedList<>();
    RaftOptions raftOptions;

    @Before
    public void init() {

        servers.add(parseServer("127.0.0.1:6666:1"));
        servers.add(parseServer("127.0.0.1:7777:2"));
        servers.add(parseServer("127.0.0.1:8888:3"));
        raftOptions = new RaftOptions();

    }

    @Test
    public void server1() throws InterruptedException {
        RaftProto.Server server;
        RaftNode raftNode;
        RpcServer rpcServer;
        RaftConsensusService raftConsensusService;

        server = parseServer("127.0.0.1:6666:1");
        raftNode = new RaftNode(raftOptions, servers, server);
        raftConsensusService = new RaftConsensusServiceImpl(raftNode);
        rpcServer = new RpcServer(server.getEndpoint().getPort());
        rpcServer.registerService(raftConsensusService);

        rpcServer.start();
        raftNode.init();
        Thread.sleep(1000000000);
    }

    @Test
    public void server2() throws InterruptedException {
        RaftProto.Server server;
        RaftNode raftNode;
        RpcServer rpcServer;
        RaftConsensusService raftConsensusService;

        server = parseServer("127.0.0.1:7777:2");
        raftNode = new RaftNode(raftOptions, servers, server);
        raftConsensusService = new RaftConsensusServiceImpl(raftNode);
        rpcServer = new RpcServer(server.getEndpoint().getPort());
        rpcServer.registerService(raftConsensusService);

        rpcServer.start();
        raftNode.init();
        Thread.sleep(1000000000);
    }

    @Test
    public void server3() throws InterruptedException {
        RaftProto.Server server;
        RaftNode raftNode;
        RpcServer rpcServer;
        RaftConsensusService raftConsensusService;

        server = parseServer("127.0.0.1:8888:3");
        raftNode = new RaftNode(raftOptions, servers, server);
        raftConsensusService = new RaftConsensusServiceImpl(raftNode);
        rpcServer = new RpcServer(server.getEndpoint().getPort());
        rpcServer.registerService(raftConsensusService);

        rpcServer.start();
        raftNode.init();
        Thread.sleep(1000000000);
    }

    private RaftProto.Server parseServer(String serverString) {
        String[] str = serverString.split(":");
        String host = str[0];
        Integer port = Integer.parseInt(str[1]);
        Integer serverId = Integer.parseInt(str[2]);
        RaftProto.Endpoint endpoint = RaftProto.Endpoint.newBuilder()
                .setHost(host).setPort(port).build();
        return RaftProto.Server.newBuilder().setEndpoint(endpoint).setServerId(serverId).build();
    }
}
