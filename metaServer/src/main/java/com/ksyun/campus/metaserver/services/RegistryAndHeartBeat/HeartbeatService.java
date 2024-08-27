package com.ksyun.campus.metaserver.services.RegistryAndHeartBeat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ksyun.campus.metaserver.domain.MetaServerNodeData;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;

@Service
public class HeartbeatService {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private ZooKeeper zooKeeper;
    private String nodePath;
    @Value("${metaServer.role}")
    private String role; // 使用int表示角色，根据配置文件决定角色
    @Value("${zookeeper.connect-string}")
    private String zookeeperHost;
    @Value("${zookeeper.session-timeout}")
    private int sessionTimeout;
    @Value("${server.ip}")
    private String ip;
    @Value("${server.port}")
    private String port;


    public HeartbeatService() {
        // 无需在构造函数中初始化 Zookeeper，初始化操作移至 connectToZookeeper 方法
    }

    /**
     * 初始化与 Zookeeper 的连接。
     * 此方法创建一个新的 Zookeeper 客户端实例，并指定一个 Watcher 处理来自 Zookeeper 的事件。
     *
     * @throws IOException 如果无法连接到 Zookeeper，则抛出异常。
     */
    private void connectToZookeeper() throws IOException {
        this.zooKeeper = new ZooKeeper(zookeeperHost, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                handleZookeeperEvent(event);
            }
        });
    }

    /**
     * 处理来自 Zookeeper 的事件。
     * 根据事件类型和状态执行不同的操作，例如重新连接、节点事件的处理等。
     *
     * @param event Zookeeper 事件对象，包含事件类型和状态等信息。
     */
    private void handleZookeeperEvent(WatchedEvent event) {
        Watcher.Event.KeeperState keeperState = event.getState();
        Watcher.Event.EventType eventType = event.getType();
        String path = event.getPath();

        switch (keeperState) {
            case SyncConnected:
                System.out.println("Successfully connected to Zookeeper");
                break;
            case Disconnected:
                System.err.println("Disconnected from Zookeeper");
                break;
            case Expired:
                System.err.println("Session expired. Reconnecting to Zookeeper...");
                try {
                    reconnectToZookeeper();
                } catch (IOException e) {
                    System.err.println("Failed to reconnect to Zookeeper");
                    e.printStackTrace();
                }
                break;
            default:
                break;
        }

        if (eventType == Watcher.Event.EventType.NodeDeleted) {
            System.out.println("Node deleted: " + path);
        } else if (eventType == Watcher.Event.EventType.NodeCreated) {
            System.out.println("Node created: " + path);
        } else if (eventType == Watcher.Event.EventType.NodeDataChanged) {
            System.out.println("Node data changed: " + path);
        } else if (eventType == Watcher.Event.EventType.NodeChildrenChanged) {
            System.out.println("Node children changed: " + path);
        }
    }

    /**
     * 重新连接到 Zookeeper。
     * 此方法首先关闭现有的 Zookeeper 连接，然后重新建立连接，并重新注册节点。
     *
     * @throws IOException 如果无法重新连接到 Zookeeper，则抛出异常。
     */
    private void reconnectToZookeeper() throws IOException {
        try {
            if (this.zooKeeper != null) {
                this.zooKeeper.close();
            }
            connectToZookeeper();
            registerNode(); // 重新注册节点
        } catch (InterruptedException | KeeperException e) {
            throw new RuntimeException("Error during Zookeeper reconnection", e);
        }
    }

    /**
     * 在 Zookeeper 中创建永久的 metaServers 节点。
     * 如果节点已经存在，则不会重复创建。
     *
     * @throws KeeperException      如果 Zookeeper 操作失败，则抛出异常。
     * @throws InterruptedException 如果线程在等待 Zookeeper 响应时被中断，则抛出异常。
     */
    @PostConstruct
    public void createPersistentMetaServersNode() throws KeeperException, InterruptedException {
        try {
            connectToZookeeper();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        String metaServersPath = "/metaServers";
        Stat stat = zooKeeper.exists(metaServersPath, false);
        if (stat == null) {
            zooKeeper.create(metaServersPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
//            System.out.println("Created persistent node: " + metaServersPath);
        }
    }

    /**
     * 在 Zookeeper 中注册节点。
     * 根据角色（role）创建临时节点，如果节点已经存在，则更新节点数据。
     *
     * @throws KeeperException      如果 Zookeeper 操作失败，则抛出异常。
     * @throws InterruptedException 如果线程在等待 Zookeeper 响应时被中断，则抛出异常。
     * @throws IOException          如果无法连接到 Zookeeper，则抛出异常。
     */
    public void registerNode() throws KeeperException, InterruptedException, IOException {
        nodePath = "/metaServers/" + role;

        // 创建包含ip和port信息的对象
        MetaServerNodeData metaServerNodeData = new MetaServerNodeData(ip, port);

        // 将对象转换为JSON字符串
        String jsonData = objectMapper.writeValueAsString(metaServerNodeData);

        // 创建临时节点，如果节点已存在则更新数据
        if (zooKeeper.exists(nodePath, false) == null) {
            zooKeeper.create(nodePath, jsonData.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } else {
            zooKeeper.setData(nodePath, jsonData.getBytes(), -1);
        }
    }

    /**
     * 向 Zookeeper 发送心跳。
     * 此方法检查节点是否存在，如果存在则更新节点数据以模拟心跳，如果不存在则重新注册节点。
     *
     * @throws KeeperException      如果 Zookeeper 操作失败，则抛出异常。
     * @throws InterruptedException 如果线程在等待 Zookeeper 响应时被中断，则抛出异常。
     */
    public void sendHeartbeat() throws KeeperException, InterruptedException {
        if (nodePath != null) {
            Stat stat = zooKeeper.exists(nodePath, false);
            if (stat != null) {
                // 创建包含ip和port信息的对象
                MetaServerNodeData metaServerNodeData = new MetaServerNodeData(ip, port);

                // 将对象转换为JSON字符串
                String jsonData;
                try {
                    jsonData = objectMapper.writeValueAsString(metaServerNodeData);
                } catch (IOException e) {
                    throw new RuntimeException("Error serializing MetaServerNodeData to JSON", e);
                }

                zooKeeper.setData(nodePath, jsonData.getBytes(), stat.getVersion());
            } else {
                try {
                    registerNode(); // 如果节点不存在，重新注册它
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            System.out.println("Heartbeat sent to Zookeeper for node: " + nodePath);
        }
    }


    /**
     * 注销 Zookeeper 节点并关闭连接。
     * 此方法删除在 Zookeeper 中注册的节点，并关闭 Zookeeper 客户端连接。
     *
     * @throws KeeperException      如果 Zookeeper 操作失败，则抛出异常。
     * @throws InterruptedException 如果线程在等待 Zookeeper 响应时被中断，则抛出异常。
     */
    @PreDestroy
    public void unregisterNode() throws KeeperException, InterruptedException {

        if (nodePath != null) {
            try {
                zooKeeper.delete(nodePath, -1);
//                System.out.println("Node deleted: " + nodePath);
            } catch (KeeperException.NoNodeException e) {
                System.out.println("Node does not exist: " + nodePath);
            }
        }

        if (zooKeeper != null) {

            //删除flagPath 下的所有子节点及子子节点
            clearNodeDataRecursively("/flagPath");

            //删除dataServerMsgCopy
            clearNodeDataRecursively("/dataServersMsgCopy");

            zooKeeper.close();
            System.out.println("Zookeeper connection closed.");

        }


    }

    private void clearNodeDataRecursively(String path) throws KeeperException, InterruptedException {
        // 获取指定路径下的子节点列表
        for (String child : zooKeeper.getChildren(path, false)) {
            // 对于每个子节点，递归调用此方法清空其子节点的数据
            clearNodeDataRecursively(path + "/" + child);
        }
        // 清空当前节点的数据
        zooKeeper.setData(path, null, -1);
    }

}