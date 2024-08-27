package com.ksyun.campus.metaserver.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ksyun.campus.metaserver.domain.MetaServerNodeData;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.Closeable;
import java.io.IOException;

@Service
public class MetaServerLeaderElection implements Closeable {

    private final LeaderLatch leaderLatch;
    private final CuratorFramework client;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private boolean isLeader = false;

    @Value("${server.ip}")
    private String serverIp;

    @Value("${server.port}")
    private String serverPort;

    public MetaServerLeaderElection(CuratorFramework client) {
        this.client = client;
        this.leaderLatch = new LeaderLatch(client, "/metaServers/leader");

        leaderLatch.addListener(new LeaderLatchListener() {
            @Override
            public void isLeader() {
                isLeader = true;
//                System.out.println("This MetaServer is now the leader.");
                // 将主节点的ip和port写入Zookeeper的leader节点中
                try {
                    writeLeaderInfoToZookeeper();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            @Override
            public void notLeader() {
                isLeader = false;
//                System.out.println("This MetaServer is not the leader.");
            }
        });
    }

    @PostConstruct
    public void start() throws Exception {
        leaderLatch.start();
    }

    @PreDestroy
    public void close() throws IOException {
        leaderLatch.close();
    }

    public boolean isLeader() {
        return isLeader;
    }

    private void writeLeaderInfoToZookeeper() throws Exception {
        // 构建 leader 节点的完整路径
        String leaderPath = "/metaServers/leader";
        MetaServerNodeData leaderInfo = new MetaServerNodeData(serverIp, serverPort);
        byte[] data = objectMapper.writeValueAsBytes(leaderInfo);
        client.setData().forPath(leaderPath, data);  // 写入数据
    }


}
