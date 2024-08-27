package com.ksyun.campus.metaserver.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ksyun.campus.metaserver.domain.DataServerMsg;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

@Service
public class MetaService {
    private final CuratorFramework client;
    private final ObjectMapper objectMapper;
//    private int roundRobinIndex = 0; // 轮询的索引
    private final Random random = new Random();

    @Value("${metaServer.replicaNumber}")
    private int replicaNumber;

    @Autowired
    public MetaService(CuratorFramework client) {
        this.client = client;
        this.objectMapper = new ObjectMapper(); // 初始化 Jackson ObjectMapper
    }

    public List<DataServerMsg> selectServerWithWeight() {
        List<DataServerMsg> dataServers = new ArrayList<>();

        try {
            // 获取 /dataservers 下的所有 zone 节点
            List<String> zones = client.getChildren().forPath("/dataservers");

            for (String zone : zones) {
                // 获取每个 zone 下的所有 rack 节点
                String zonePath = "/dataservers/" + zone;
                List<String> racks = client.getChildren().forPath(zonePath);

                for (String rack : racks) {
                    // 获取每个 rack 下的所有 DataServer 节点
                    String rackPath = zonePath + "/" + rack;
                    List<String> serverNodes = client.getChildren().forPath(rackPath);

                    // 遍历每个节点，读取数据并反序列化为 DataServerMsg 对象
                    for (String serverNode : serverNodes) {
                        String fullPath = rackPath + "/" + serverNode;
                        byte[] nodeData = client.getData().forPath(fullPath);
                        DataServerMsg dataServerMsg = objectMapper.readValue(nodeData, DataServerMsg.class);
                        dataServers.add(dataServerMsg);
                    }
                }
            }

            // 如果没有可用的服务器，返回空列表
            if (dataServers.isEmpty()) {
                System.out.println("没有可用的服务器");
                return new ArrayList<>();
            }

            // 计算所有服务器的总权重
            int totalWeight = 0;
            List<Integer> weights = new ArrayList<>();

            // 找到最小和最大剩余容量
            int minCapacity = Integer.MAX_VALUE;
            int maxCapacity = Integer.MIN_VALUE;

            for (DataServerMsg server : dataServers) {
                int remainingCapacity = server.getCapacity() - server.getUseCapacity();
                if (remainingCapacity < minCapacity) {
                    minCapacity = remainingCapacity;
                }
                if (remainingCapacity > maxCapacity) {
                    maxCapacity = remainingCapacity;
                }
            }

            // 如果所有服务器的剩余容量都是0，返回空列表
            if (maxCapacity < 0) {
                System.out.println("所有服务器的剩余容量为0，无法进行负载均衡选择");
                return new ArrayList<>();
            }

            // 归一化并计算权重
            for (DataServerMsg server : dataServers) {
                int remainingCapacity = server.getCapacity() - server.getUseCapacity();
                int normalizedWeight;

                if (minCapacity == maxCapacity) {
                    normalizedWeight = 1;  // 如果所有服务器剩余容量相同，权重设置为1
                } else {
                    normalizedWeight = 1 + (int) ((double) (remainingCapacity - minCapacity) / (maxCapacity - minCapacity) * 99);
                }

                weights.add(normalizedWeight);
                totalWeight += normalizedWeight;
            }

            // 选择最多3个服务器
            List<DataServerMsg> selectedServers = new ArrayList<>();
            int numServersToSelect = Math.min(replicaNumber, dataServers.size());

            for (int j = 0; j < numServersToSelect; j++) {
                // 生成一个随机数，根据权重选择服务器
                int randomWeight = random.nextInt(totalWeight);

                for (int i = 0; i < dataServers.size(); i++) {
                    randomWeight -= weights.get(i);
                    if (randomWeight < 0) {
                        selectedServers.add(dataServers.get(i));
                        // 减少 totalWeight 以适应下一个选择
                        totalWeight -= weights.get(i);
                        dataServers.remove(i);
                        weights.remove(i);
                        break;
                    }
                }
            }

            return selectedServers;

        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("获取服务器信息时出错");
            return new ArrayList<>();
        }
    }


    //获取所有可用的dataServer的列表
    public List<DataServerMsg> getAllDataServers() {
        List<DataServerMsg> dataServers = new ArrayList<>();

        try {
            // 获取 /dataservers 下的所有 zone 节点
            List<String> zones = client.getChildren().forPath("/dataservers");

            for (String zone : zones) {
                // 获取每个 zone 下的所有 rack 节点
                String zonePath = "/dataservers/" + zone;
                List<String> racks = client.getChildren().forPath(zonePath);

                for (String rack : racks) {
                    // 获取每个 rack 下的所有 DataServer 节点
                    String rackPath = zonePath + "/" + rack;
                    List<String> serverNodes = client.getChildren().forPath(rackPath);

                    // 遍历每个节点，读取数据并反序列化为 DataServerMsg 对象
                    for (String serverNode : serverNodes) {
                        String fullPath = rackPath + "/" + serverNode;
                        byte[] nodeData = client.getData().forPath(fullPath);
                        DataServerMsg DataServerMsg = objectMapper.readValue(nodeData, DataServerMsg.class);
                        dataServers.add(DataServerMsg);
                    }
                }
            }
            return dataServers;

        } catch (Exception e) {
            // 处理异常情况
            e.printStackTrace();
            return null;
        }
    }


    // 检测给定路径中的所有父目录是否存在
    public boolean isParentPathExists(String path) {
        try {
            String nodePath = "/metadata/" + path;

            // 获取父目录路径
            String parentPath = nodePath.substring(0, nodePath.lastIndexOf('/'));

            // 如果路径是根目录，直接返回 true
            if (parentPath.isEmpty() || parentPath.equals("/metadata")) {
                return true;
            }

            // 检查父目录是否存在
            Stat stat = client.checkExists().forPath(parentPath);

            if (stat != null) {
                return true; // 当前父目录存在
            } else {
                // 递归检查上一层父目录是否存在
                return isParentPathExists(parentPath.substring("/metadata/".length()));
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false; // 如果发生异常，假定父目录不存在
        }
    }


    //检测给定路径的文件/目录是否存在
    public boolean isMetadataExists(String path) {
        try {
            String nodePath="/metadata/"+path;
            // 检查路径是否存在
            Stat stat = client.checkExists().forPath(nodePath);
            return stat != null; // 如果路径存在，返回 true
        } catch (Exception e) {
            e.printStackTrace();
            return false; // 如果发生异常，假定元数据不存在
        }
    }
}



//// 获取副本，负载均衡策略为轮询选择，返回最多三个dataServer副本的列表
//public List<DataServerMsg> pickBalancedSlaveDataServers() {
//    List<DataServerMsg> dataServers = new ArrayList<>();
//
//    try {
//        // 获取 /dataservers 下的所有 zone 节点
//        List<String> zones = client.getChildren().forPath("/dataservers");
//
//        for (String zone : zones) {
//            // 获取每个 zone 下的所有 rack 节点
//            String zonePath = "/dataservers/" + zone;
//            List<String> racks = client.getChildren().forPath(zonePath);
//
//            for (String rack : racks) {
//                // 获取每个 rack 下的所有 DataServer 节点
//                String rackPath = zonePath + "/" + rack;
//                List<String> serverNodes = client.getChildren().forPath(rackPath);
//
//                // 遍历每个节点，读取数据并反序列化为 DataServerMsg 对象
//                for (String serverNode : serverNodes) {
//                    String fullPath = rackPath + "/" + serverNode;
//                    byte[] nodeData = client.getData().forPath(fullPath);
//                    DataServerMsg DataServerMsg = objectMapper.readValue(nodeData, DataServerMsg.class);
//                    dataServers.add(DataServerMsg);
//                }
//            }
//        }
//
//        // 如果没有可用的服务器，返回 null
//        if (dataServers.isEmpty()) {
//            return null;
//        }
//
//        // 使用轮询算法选择最多replicaNumber个服务器
//        List<DataServerMsg> selectedServers = new ArrayList<>();
//        int numServersToSelect = Math.min(replicaNumber, dataServers.size()); // 至多选择三个
//
//        for (int i = 0; i < numServersToSelect; i++) {
//            DataServerMsg selectedServer = dataServers.get(roundRobinIndex);
//            selectedServers.add(selectedServer);
//
//            // 更新轮询索引
//            roundRobinIndex = (roundRobinIndex + 1) % dataServers.size();
//        }
//
//        return selectedServers;
//
//    } catch (Exception e) {
//        // 处理异常情况
//        e.printStackTrace();
//        return null;
//    }
//}

