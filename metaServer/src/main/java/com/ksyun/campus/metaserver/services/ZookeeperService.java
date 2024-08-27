package com.ksyun.campus.metaserver.services;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ksyun.campus.metaserver.domain.ConfigData;
import com.ksyun.campus.metaserver.domain.DataServerMsg;
import com.ksyun.campus.metaserver.domain.StatInfo;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.List;

@Service
public class ZookeeperService {

    @Value("${zookeeper.connect-string}")
    private String connectString;

    private CuratorFramework client;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @PostConstruct
    public void init() {
        client = CuratorFrameworkFactory.newClient(connectString, new ExponentialBackoffRetry(3000, 3));
        client.start();
    }

    //注册存放元数据的metadata节点
    @PostConstruct
    public void registerMetaDataNode(){
        try {
            if (client.checkExists().forPath("/metadata") == null) {
                client.create().creatingParentsIfNeeded().forPath("/metadata", null);
            } else {
                client.setData().forPath("/metadata", null);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // 检查节点是否存在
    public boolean nodeExists(String path) throws Exception {
        return client.checkExists().forPath(path) != null;
    }
    // 注册注册 StatInfo 节点
    public void registerNode(String path, StatInfo statInfo) throws Exception {
        String data = objectMapper.writeValueAsString(statInfo);
        if (client.checkExists().forPath(path) == null) {
            client.create().creatingParentsIfNeeded().forPath(path, data.getBytes());
        } else {
            client.setData().forPath(path, data.getBytes());
        }
    }

    // 用于注册副本信息 List<ConfigData>
    public void registerNode(String path, List<ConfigData> configDataList) throws Exception {
        String data = objectMapper.writeValueAsString(configDataList);
        if (client.checkExists().forPath(path) == null) {
            client.create().creatingParentsIfNeeded().forPath(path, data.getBytes());
        } else {
            client.setData().forPath(path, data.getBytes());
        }
    }

    // 用于注册String类型的flagPath作为临时节点
    public void registerEphemeralNode(String path, String data) throws Exception {
        if (client.checkExists().forPath(path) == null) {
            client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(path, data.getBytes());
        } else {
            client.setData().forPath(path, data.getBytes());
        }
    }


    // 获取节点数据
    public StatInfo getNodeData(String path) throws Exception {
        byte[] data = client.getData().forPath(path);
        return objectMapper.readValue(data, StatInfo.class);
    }

    //获取副本数据
    // 获取存储在指定路径下的 ConfigData 列表
    public List<ConfigData> getConfigData(String path) throws Exception {
        // 检查节点是否存在
        if (client.checkExists().forPath(path) == null) {
            return null;
        }
        // 获取节点数据
        byte[] data = client.getData().forPath(path);
        // 反序列化 JSON 数据为 List<ConfigData>
        return objectMapper.readValue(data, new TypeReference<List<ConfigData>>() {});
    }

    public void registerDataServerMsgNode(String path, List<DataServerMsg> dataServerMsgList) throws Exception {
        // 将 List<DataServerMsg> 序列化为 JSON 字符串
        String data = objectMapper.writeValueAsString(dataServerMsgList);

        // 检查节点是否存在
        if (client.checkExists().forPath(path) == null) {
            // 如果节点不存在，创建节点并写入数据
            client.create().creatingParentsIfNeeded().forPath(path, data.getBytes());
        } else {
            // 如果节点存在，直接写入数据
            client.setData().forPath(path, data.getBytes());
        }
    }


    public List<DataServerMsg> getDataServerMsgList(String path) throws Exception {
        // 检查节点是否存在
        if (client.checkExists().forPath(path) == null) {
            return null;
        }
        // 获取节点数据
        byte[] data = client.getData().forPath(path);
        // 反序列化 JSON 数据为 List<DataServerMsg>
        return objectMapper.readValue(data, new TypeReference<List<DataServerMsg>>() {});
    }


    // 检查节点是否有子节点
    public boolean hasChildren(String path) throws Exception {
        List<String> children = client.getChildren().forPath(path);
        return !children.isEmpty();
    }

    // 递归删除节点及其子节点
    public void deleteNodeRecursively(String path) throws Exception {
        List<String> children = getChildren(path);
        for (String child : children) {
            String childPath = path + "/" + child;
            deleteNodeRecursively(childPath);
        }
        deleteNode(path);
    }

    // 删除节点
    public void deleteNode(String path) throws Exception {
        client.delete().forPath(path);
    }

    // 获取子节点
    public List<String> getChildren(String path) throws Exception {
        return client.getChildren().forPath(path);
    }

    //更新dataServer的已使用容量useCapacity
    //flag表示此次是加使用容量还是减使用容量，1加，-1减
    public void updateDataServerUseCapacity(String path, int capacity, int flag) {
        try {
            // 构建完整的DataServer节点路径
            String dataServerNodePath =path;

            // 读取当前节点的数据
            byte[] dataBytes = client.getData().forPath(dataServerNodePath);
            DataServerMsg dataServerMsg = objectMapper.readValue(dataBytes, DataServerMsg.class);

            // 根据flag更新useCapacity
            if (flag == 1) {  // 增加useCapacity
                dataServerMsg.setUseCapacity(dataServerMsg.getUseCapacity() + capacity);
            } else if (flag == -1) {  // 减少useCapacity
                dataServerMsg.setUseCapacity(dataServerMsg.getUseCapacity() - capacity);
            }

            // 将更新后的DataServerMsg写回Zookeeper
            byte[] updatedDataBytes = objectMapper.writeValueAsBytes(dataServerMsg);
            client.setData().forPath(dataServerNodePath, updatedDataBytes);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public CuratorFramework getClient() {
        return client;
    }

    public void close() {
        client.close();
    }

}
