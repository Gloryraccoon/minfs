package com.ksyun.campus.metaserver.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ksyun.campus.metaserver.domain.*;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.*;
import java.util.stream.Collectors;

@Service
public class FsckServices {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    private CuratorFramework client;

    @Autowired
    private MetaServerLeaderElection metaServerLeaderElection;

    @Autowired
    private MetaService metaService;

    @Autowired
    private  ZookeeperService zookeeperService;

    @Value("${metaServer.replicaNumber}")
    private int replicaNumber;

    // todo 全量扫描文件列表
    // todo 检查文件副本数量是否正常
    // todo 更新文件副本数：3副本、2副本、单副本
    // todo 记录当前检查结果
    // todo 将错误的或者丢失的副本恢复

    @Scheduled(initialDelay = 20000, fixedDelay = 120000)
    public void fsckTask() {
        // 只有当当前 MetaServer 是主节点时，才执行 fsckTask
        if (metaServerLeaderElection.isLeader()) {
//            System.out.println("当前 MetaServer 是主节点，执行 fsckTask");
            DataServerLost();
            isReplicaNumberCorrect();
            updateFileNumber();
        } else {
//            System.out.println("当前 MetaServer 不是主节点，跳过 fsckTask");
        }
    }


//    //该服务用于记录正常情况下的所有dataServer服务的信息
    @Scheduled(initialDelay = 5000, fixedDelay = 30000)
    public void register() {
        try {
            // 获取所有可用的 DataServerMsg 信息
            List<DataServerMsg> dataServerMsgList = metaService.getAllDataServers();

            // 将列表序列化为 JSON 字符串
            ObjectMapper objectMapper = new ObjectMapper();
            String jsonData = objectMapper.writeValueAsString(dataServerMsgList);

            // 注册到 Zookeeper 的 /dataServerMsgCopy 路径下
            String zkPath = "/dataServerMsgCopy";

            // 检查节点是否存在
            if (client.checkExists().forPath(zkPath) == null) {
                // 如果节点不存在，则创建
                client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(zkPath, jsonData.getBytes());
//                System.out.println("DataServerMsgCopy 信息已成功注册到 Zookeeper 的 " + zkPath + " 路径下");
            } else {
                // 如果节点存在，获取当前存储的数据
                byte[] existingData = client.getData().forPath(zkPath);
                List<DataServerMsg> existingList = objectMapper.readValue(existingData,
                        objectMapper.getTypeFactory().constructCollectionType(List.class, DataServerMsg.class));

                // 比较并合并新的 DataServerMsg 信息
                for (DataServerMsg newMsg : dataServerMsgList) {
                    boolean exists = existingList.stream().anyMatch(
                            msg -> msg.getHost().equals(newMsg.getHost()) && msg.getPort() == newMsg.getPort()
                    );
                    if (!exists) {
                        existingList.add(newMsg);
                    }
                }

                // 将更新后的列表序列化并写回 Zookeeper
                String updatedJsonData = objectMapper.writeValueAsString(existingList);
                client.setData().forPath(zkPath, updatedJsonData.getBytes());

//                System.out.println("DataServerMsgCopy 信息已成功更新到 Zookeeper 的 " + zkPath + " 路径下");
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("注册或更新 DataServerMsgCopy 信息到 Zookeeper 失败。");
        }
    }


    // 检测并处理 DataServer 丢失
    private void DataServerLost() {
        try {
            // 获取所有可用的 DataServer 信息
            List<DataServerMsg> dataServerMsgList = metaService.getAllDataServers();

            // 获取 /dataServerMsgCopy 路径下的信息
            String zkCopyPath = "/dataServerMsgCopy";
            byte[] data = client.getData().forPath(zkCopyPath);
            ObjectMapper objectMapper = new ObjectMapper();
            List<DataServerMsg> dataServerMsgCopyList = objectMapper.readValue(data,
                    objectMapper.getTypeFactory().constructCollectionType(List.class, DataServerMsg.class));

            // 找出在 dataServerMsgCopyList 中，但不在 dataServerMsgList 中的 DataServer
            List<DataServerMsg> differentServers = new ArrayList<>();//记录无效的dataServer
            List<DataServerMsg> sameServers = new ArrayList<>(); // 用于有效的dataServer
            for (DataServerMsg copyServer : dataServerMsgCopyList) {
                boolean exists = false;
                for (DataServerMsg currentServer : dataServerMsgList) {
                    if (copyServer.getHost().equals(currentServer.getHost()) &&
                            copyServer.getPort() == currentServer.getPort()) {
                        exists = true;
                        sameServers.add(copyServer); // 记录相同的服务器
                        break;
                    }
                }
                if (!exists) {
                    differentServers.add(copyServer);
                }
            }

            // 如果存在不同的 DataServer，则需要检查和转移副本
            if (!differentServers.isEmpty()) {
                List<String> directories = client.getChildren().forPath("/metadata");
                Map<String, List<StatInfo>> fileStatInfoMap = new HashMap<>(); // 用于保存目录和文件的映射

                for (String directory : directories) {
                    // 收集当前目录下的文件信息
                    List<StatInfo> statInfoList = new ArrayList<>();
                    traverseNodes("/metadata/" + directory, statInfoList, true);

                    // 如果当前目录下有文件，保存到 HashMap 中
                    if (!statInfoList.isEmpty()) {
                        fileStatInfoMap.put(directory, statInfoList);
                    }
                }
                dealDataServerLost(fileStatInfoMap, differentServers, sameServers);

                // 将 differentServers 对应的dataServer在 /dataServerMsgCopy 节点的数据去除
                for (DataServerMsg server : differentServers) {
                    dataServerMsgCopyList.remove(server);
                }
                // 将更新后的 dataServerMsgCopyList 写回到 Zookeeper 节点
                byte[] updatedData = objectMapper.writeValueAsBytes(dataServerMsgCopyList);
                client.setData().forPath(zkCopyPath, updatedData);

//                System.out.println("已删除丢失的 dataServer 服务信息");

            } else {
//                System.out.println("dataServer服务未丢失");
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("检测dataServer服务是否丢失失败");
        }
    }

    private void dealDataServerLost(Map<String, List<StatInfo>> fileStatInfoMap,
                                    List<DataServerMsg> differentServers, List<DataServerMsg> sameServers) {


        for (Map.Entry<String, List<StatInfo>> entry : fileStatInfoMap.entrySet()) {
            for (StatInfo statInfo : entry.getValue()) {
                String fileConfigPath = "/metadata/" + entry.getKey() + statInfo.getPath() + "/config";
                String statInfoPath = "/metadata/" + entry.getKey() + statInfo.getPath(); // StatInfo 存储的路径
//                System.out.println("fileConfigPath: " + fileConfigPath);

                try {
                    // 读取 config 节点的数据来获取文件的副本列表
                    byte[] configDataBytes = client.getData().forPath(fileConfigPath);
                    List<ConfigData> configDataList = objectMapper.readValue(configDataBytes,
                            objectMapper.getTypeFactory().constructCollectionType(List.class, ConfigData.class));

                    // 删除 ConfigData 中与 differentServers 匹配的数据
                    List<ConfigData> configsToRemove = new ArrayList<>();
                    ConfigData targetServer = null;

                    for (ConfigData config : configDataList) {
                        boolean isMatched = false;
                        for (DataServerMsg differentServer : differentServers) {
                            if (config.getIp().equals(differentServer.getHost()) &&
                                    config.getPort() == differentServer.getPort() &&
                                    config.getZone().equals(differentServer.getZone()) &&
                                    config.getRack().equals(differentServer.getRack())) {
                                configsToRemove.add(config);
                                isMatched = true;
                                break;
                            }
                        }

                        // 如果没有匹配到 differentServer, 将该 ConfigData 作为 targetServer
                        if (!isMatched && targetServer == null) {
                            targetServer = config;
                        }
                    }

                    if (configsToRemove.isEmpty()) {
                        continue;  // 如果没有无效服务器，跳过
                    }

                    // 从 configDataList 中移除匹配的数据
                    configDataList.removeAll(configsToRemove);

                    // 复制 sameServers 列表以避免修改原列表
                    List<DataServerMsg> sameServersCopy = new ArrayList<>(sameServers);

                    // 从 sameServers 中移除与 config 中相同的dataServer，并记录被移除的dataServer信息
                    List<ConfigData> removedServers = new ArrayList<>();
                    List<ConfigData> finalConfigDataList = configDataList;
                    sameServersCopy.removeIf(sameServer -> {
                        boolean match = finalConfigDataList.stream().anyMatch(config ->
                                config.getIp().equals(sameServer.getHost()) &&
                                        config.getPort() == sameServer.getPort() &&
                                        config.getZone().equals(sameServer.getZone()) &&
                                        config.getRack().equals(sameServer.getRack()));
                        if (!match) {
                            removedServers.add(new ConfigData(sameServer.getHost(), sameServer.getPort(), sameServer.getZone(), sameServer.getRack()));
                        }
                        return match; // 如果匹配到存储文件副本的DataServer，从sameServers中移除
                    });


                    //此时sameServers为没有存储文件副本的可用DataServer
                    //removedServers为存储文件副本的可用DataServer


                    if (sameServers.isEmpty()) {
                        System.out.println("没有可用的dataServer服务器");
                    } else {
                        // 添加新的副本信息
                        int serversToAdd = Math.min(differentServers.size(), sameServersCopy.size());
                        if (serversToAdd < differentServers.size()) {
                            System.out.println("警告: 可用的服务器数量不足以完全替代丢失的副本，期望 " + differentServers.size() + " 个，但只找到了 " + serversToAdd + " 个。");
                        }

                        List<ConfigData> recoveryConfigDataList = new ArrayList<>();
                        for (int i = 0; i < serversToAdd; i++) {
                            DataServerMsg newServer = sameServersCopy.get(i);
                            ConfigData newConfigData = new ConfigData(newServer.getHost(), newServer.getPort(), newServer.getZone(), newServer.getRack());
                            recoveryConfigDataList.add(newConfigData);
                        }

                        // 合并 recoveryConfigDataList 到 configDataList 中，并去重
                        configDataList.addAll(recoveryConfigDataList);
                        configDataList = configDataList.stream().distinct().collect(Collectors.toList());

                        // 确保最终副本数量不超过 maxReplicaCount
                        if (configDataList.size() > replicaNumber) {
                            configDataList = configDataList.subList(0, replicaNumber);
                        }

//                        System.out.println(recoveryConfigDataList.size());

                        // 将合并后的 configDataList 写回到 ZooKeeper
                        byte[] updatedConfigDataBytes = objectMapper.writeValueAsBytes(configDataList);
                        client.setData().forPath(fileConfigPath, updatedConfigDataBytes);

                        // 更新 StatInfo 的 replicaData 列表
                        List<ReplicaData> updatedReplicaData = configDataList.stream()
                                .map(config -> new ReplicaData(UUID.randomUUID().toString(),
                                        "/" + config.getIp() + "/" + config.getPort() + "/" + config.getZone() + "/" + config.getRack(),
                                        statInfo.getPath()))
                                .collect(Collectors.toList());

                        // 设置更新后的 replicaData 列表
                        statInfo.setReplicaData(updatedReplicaData);

                        // 将更新后的 StatInfo 写回到 ZooKeeper
                        byte[] updatedStatInfoBytes = objectMapper.writeValueAsBytes(statInfo);
                        client.setData().forPath(statInfoPath, updatedStatInfoBytes);


                        if (!sameServersCopy.isEmpty()) {
                            // 选择一个存储文件副本的有效DataServer作为请求的目标服务器
                            String recoveryUrl = "http://" + targetServer.getIp() + ":" + targetServer.getPort() +
                                    "/recoveryfile?path="+statInfo.getPath();
//                            System.out.println("recoveryUrl: "+recoveryUrl);
                            HttpHeaders headers = new HttpHeaders();
                            headers.set("fileSystemName", entry.getKey());
                            HttpEntity<List<ConfigData>> requestEntity = new HttpEntity<>(recoveryConfigDataList, headers);
                            RestTemplate restTemplate = new RestTemplate();
                            try {
                                ResponseEntity<String> response = restTemplate.postForEntity(
                                        recoveryUrl,
                                        requestEntity,
                                        String.class
                                );

                                if (!response.getStatusCode().is2xxSuccessful()) {
                                    System.out.println("数据转移失败: " + response.getBody());
                                } else {
                                    System.out.println("数据转移成功: " + recoveryUrl);
                                }
                            } catch (Exception e) {
                                System.out.println("调用恢复接口时发生错误: " + e.getMessage());
                            }
                        } else {
                            System.out.println("没有可用的 dataServer 服务器来进行数据转移。");
                        }
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("读取或解析 config 节点失败: " + fileConfigPath);
                }
            }
        }
    }


    //检测并更新副本数量
    private void isReplicaNumberCorrect() {
        try {
            List<String> directories = client.getChildren().forPath("/metadata");

            Map<String, List<StatInfo>> fileStatInfoMap = new HashMap<>(); // 用于保存目录和文件的映射

            for (String directory : directories) {
                // 收集当前目录下的文件信息
                List<StatInfo> statInfoList = new ArrayList<>();
                traverseNodes("/metadata/" + directory, statInfoList, true);

                // 如果当前目录下有文件，保存到 HashMap 中
                if (!statInfoList.isEmpty()) {
                    fileStatInfoMap.put(directory, statInfoList);
                }
            }

            for (Map.Entry<String, List<StatInfo>> entry : fileStatInfoMap.entrySet()) {
//            System.out.println("目录: " + entry.getKey());
                for (StatInfo statInfo : entry.getValue()) {
                    String nodePath = "/metadata/" + entry.getKey() + statInfo.getPath();
                    String fileConfigPath = "/metadata/" + entry.getKey() + statInfo.getPath() + "/config";
//                System.out.println("  文件: " + statInfo.getPath() + ", 类型: " + statInfo.getType());

                    // 读取 config 节点的数据来获取文件的副本列表
                    byte[] configDataBytes = client.getData().forPath(fileConfigPath);
                    List<ConfigData> configDataList = objectMapper.readValue(configDataBytes,
                            objectMapper.getTypeFactory().constructCollectionType(List.class, ConfigData.class));

                    // 检查 config 数据的个数是否为 replicaNumber
                    if (configDataList.size() == replicaNumber) {
//                        System.out.println(nodePath + " 副本数量正确");
                    } else if (configDataList.size() < replicaNumber) {
                        System.out.println(nodePath + " 副本数量<" + replicaNumber + ",当前数量为： " + configDataList.size());
                        dealReplicaNumberWrong(entry.getKey(), nodePath, replicaNumber - configDataList.size());
                    } else {
                        System.out.println(nodePath + " 副本数量>" + replicaNumber + ",当前数量为： " + configDataList.size());
                        //TODO: 副本数量>replicaNumber如何处理
                    }

                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("副本数量检测失败");
        }
    }

    //实现副本的备份，number为需要备份的副本数
    private void dealReplicaNumberWrong(String fileSystemName,String nodePath, int number) {
        try {
            // 获取所有可用的 DataServer 信息
            List<DataServerMsg> dataServerMsgList = metaService.getAllDataServers();

            // 获取当前节点的 config 数据
            String fileConfigPath = nodePath + "/config";
            byte[] configDataBytes = client.getData().forPath(fileConfigPath);
            List<ConfigData> configDataList = objectMapper.readValue(configDataBytes,
                    objectMapper.getTypeFactory().constructCollectionType(List.class, ConfigData.class));

            // 过滤掉dataServerMsgList中与 configDataList 中相同的 DataServer，即找到未存储该文件副本的可用的dataServer
            List<DataServerMsg> availableServers = new ArrayList<>();
            List<ConfigData> removedServers = new ArrayList<>(); // 用于记录被过滤掉的dataServer

            for (DataServerMsg server : dataServerMsgList) {
                boolean exists = false;
                for (ConfigData config : configDataList) {
                    if (server.getHost().equals(config.getIp()) &&
                            server.getPort() == config.getPort() &&
                            server.getZone().equals(config.getZone()) &&
                            server.getRack().equals(config.getRack())) {
                        exists = true;
                        // 将被过滤掉的 DataServerMsg 转换为 ConfigData 并添加到 removedServers 中，即保留存储文件副本的dataServer信息
                        removedServers.add(new ConfigData(server.getHost(), server.getPort(), server.getZone(), server.getRack()));
                        break;
                    }
                }
                if (!exists) {
                    availableServers.add(server);
                }
            }


            if (availableServers.isEmpty()) {
                System.out.println("无可用的dataServer服务器");
            } else {

                // 从 availableServers 中选择 number 数量的服务器
                int serversToAdd = Math.min(number, availableServers.size());
//                System.out.println("serversToAdd: "+serversToAdd);

                // 构建新的 ConfigData 列表
                List<ConfigData> recoveryConfigDataList = new ArrayList<>();
                // 从 availableServers 中选择 serverToAdd 数量的服务器,并添加到 recoveryConfigDataList 中
                for (int i = 0; i < serversToAdd; i++) {
                    DataServerMsg newServer = availableServers.get(i);
                    ConfigData newConfig = new ConfigData(newServer.getHost(), newServer.getPort(), newServer.getZone(), newServer.getRack());

                    // 与 removedServers 进行比对，确保不添加与 removedServers 相同的数据
                    boolean isDuplicate = removedServers.stream().anyMatch(config ->
                            config.getIp().equals(newConfig.getIp()) &&
                                    config.getPort() == newConfig.getPort() &&
                                    config.getZone().equals(newConfig.getZone()) &&
                                    config.getRack().equals(newConfig.getRack()));

                    if (!isDuplicate) {
                        recoveryConfigDataList.add(newConfig);
                    }
                }

                // 去重，确保没有重复的 dataServer
                recoveryConfigDataList = recoveryConfigDataList.stream().distinct().collect(Collectors.toList());

                // 将新的 ConfigData 添加到现有的 configDataList 中
                configDataList.addAll(recoveryConfigDataList);

                // 将更新后的 configDataList 写回到 Zookeeper，而不是覆盖现有数据
                byte[] updatedConfigDataBytes = objectMapper.writeValueAsBytes(configDataList);
                client.setData().forPath(fileConfigPath, updatedConfigDataBytes);


                // 更新文件的replicaData
                byte[] statInfoData = client.getData().forPath(nodePath);
                StatInfo statInfo = objectMapper.readValue(statInfoData, StatInfo.class);

                // 将 recoveryConfigDataList 转换为 ReplicaData 列表
                List<ReplicaData> newReplicaData = recoveryConfigDataList.stream()
                        .map(config -> new ReplicaData(UUID.randomUUID().toString(),
                                "/" + config.getIp() + "/" + config.getPort() + "/" + config.getZone() + "/" + config.getRack(),
                                statInfo.getPath()))
                        .collect(Collectors.toList());

                // 将新的 ReplicaData 添加到现有的 replicaData 列表中
                List<ReplicaData> existingReplicaData = statInfo.getReplicaData();
                existingReplicaData.addAll(newReplicaData);

                // 确保没有重复的 ReplicaData
                existingReplicaData = existingReplicaData.stream().distinct().collect(Collectors.toList());

                // 设置更新后的 replicaData 列表
                statInfo.setReplicaData(existingReplicaData);

                // 将更新后的 StatInfo 写回到 Zookeeper
                byte[] updatedStatInfoBytes = objectMapper.writeValueAsBytes(statInfo);
                client.setData().forPath(nodePath, updatedStatInfoBytes);


                // 调用 dataServer 接口进行数据备份
                // 选择一个存储文件副本的有效DataServer作为请求的目标服务器
                String recoveryUrl = "http://" + removedServers.get(0).getIp() + ":" + removedServers.get(0).getPort() +
                        "/recoveryfile?path="+statInfo.getPath();
                HttpHeaders headers = new HttpHeaders();
                headers.set("fileSystemName",fileSystemName);
                HttpEntity<List<ConfigData>> requestEntity = new HttpEntity<>(recoveryConfigDataList, headers);
                RestTemplate restTemplate = new RestTemplate();
                try {
                    ResponseEntity<String> response = restTemplate.postForEntity(
                            recoveryUrl,
                            requestEntity,
                            String.class
                    );
                    if (!response.getStatusCode().is2xxSuccessful()) {
                        System.out.println("数据转移失败: " + response.getBody());
                    } else {
                        if (serversToAdd < number) {
                            System.out.println("DataServer 服务数量不足，当前副本数为：" + configDataList.size());
                        } else {
                            System.out.println(nodePath + " 副本已修复，当前副本数为：" + configDataList.size());
                        }
                    }
                } catch (Exception e) {
                    System.out.println("调用恢复接口时发生错误: " + e.getMessage());
                }
            }


            // 将更新后的 configDataList 写回到 Zookeeper
            byte[] updatedConfigDataBytes = objectMapper.writeValueAsBytes(configDataList);
            client.setData().forPath(fileConfigPath, updatedConfigDataBytes);


        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("处理副本数量不足时出错: " + nodePath);
        }
    }


    //获得所有type为file的节点元数据
    private void traverseNodes(String path, List<StatInfo> statInfoList, boolean isFileSystemName) {
        try {
            // 获取当前路径下的子节点
            List<String> children = client.getChildren().forPath(path);

            // 如果没有子节点，直接跳过
            if (children.isEmpty()) {
//                System.out.println("跳过无子节点的路径: " + path);
            } else {
                if (isFileSystemName) {
                    for (String child : children) {
                        traverseNodes(path + "/" + child, statInfoList, false);
                    }
                } else {
                    // 获取当前节点的 StatInfo
                    byte[] data = client.getData().forPath(path);
                    ObjectMapper objectMapper = new ObjectMapper();
                    StatInfo statInfo = objectMapper.readValue(data, StatInfo.class);

                    // 如果当前节点的type是file则存储到 List 中
                    if (statInfo.getType() == FileType.File) {
                        statInfoList.add(statInfo);
//                        System.out.println("保存文件: " + path);
                    } else if (statInfo.getType() == FileType.Directory) {
                        // 如果当前节点是目录类型且有子节点，继续递归遍历
                        for (String child : children) {
                            traverseNodes(path + "/" + child, statInfoList, false);
                        }
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("遍历节点失败: " + path);
        }
    }


    //更新文件的数量
    private void updateFileNumber() {
        try {
            // 1. 获取所有 DataServer 并将 fileTotal 属性重置为 0
            List<DataServerMsg> dataServers = metaService.getAllDataServers();
            if (dataServers != null) {
                for (DataServerMsg dataServerMsg : dataServers) {
                    dataServerMsg.setFileTotal(0); // 重置 fileTotal 为 0
                    dataServerMsg.setUseCapacity(0);

                    // 更新 ZooKeeper 中的节点数据
                    String dataServerPath = "/dataservers/" + dataServerMsg.getZone() + "/" + dataServerMsg.getRack() + "/"
                            + dataServerMsg.getHost() + ":" + dataServerMsg.getPort();
                    client.setData().forPath(dataServerPath, objectMapper.writeValueAsBytes(dataServerMsg));
                }
            } else {
                System.out.println("未能获取 DataServer 列表。");
                return;
            }

            // 2. 获取所有文件的 StatInfo 列表
            String fileMetaDataCopy = "/fileMetaDataCopy";
            List<String> directories = client.getChildren().forPath("/metadata");
            Map<String, List<StatInfo>> fileStatInfoMap = new HashMap<>();

            for (String directory : directories) {
                List<StatInfo> statInfoList = new ArrayList<>();
                traverseNodes("/metadata/" + directory, statInfoList, true);

                if (!statInfoList.isEmpty()) {
                    fileStatInfoMap.put(directory, statInfoList);
                }
            }

            // 3. 根据文件的 config 列表更新 DataServer 的 fileTotal
            for (Map.Entry<String, List<StatInfo>> entry : fileStatInfoMap.entrySet()) {
                String key = entry.getKey();
                for (StatInfo statInfo : entry.getValue()) {
                    String configDataPath = "/metadata/" + key + statInfo.getPath() + "/config";

                    long capacity= statInfo.getSize();

                    List<ConfigData> configDataList = zookeeperService.getConfigData(configDataPath);

                    if (configDataList != null) {
                        for (ConfigData configData : configDataList) {
                            String dataServerPath = "/dataservers/" + configData.getZone() + "/" + configData.getRack() + "/"
                                    + configData.getIp() + ":" + configData.getPort();

                            // 找到对应的 DataServerMsg 对象并更新 fileTotal
                            for (DataServerMsg dataServerMsg : dataServers) {
                                if (dataServerMsg.getHost().equals(configData.getIp())
                                        && dataServerMsg.getPort() == configData.getPort()
                                        && dataServerMsg.getZone().equals(configData.getZone())
                                        && dataServerMsg.getRack().equals(configData.getRack())) {

                                    dataServerMsg.setUseCapacity((int)capacity+dataServerMsg.getUseCapacity());
                                    dataServerMsg.setFileTotal(dataServerMsg.getFileTotal() + 1);
                                    byte[] updatedDataServerMsg = objectMapper.writeValueAsBytes(dataServerMsg);
                                    client.setData().forPath(dataServerPath, updatedDataServerMsg);

                                    break; // 一旦找到匹配的 DataServerMsg 就可以退出内层循环
                                }
                            }
                        }
                    } else {
                        System.out.println("ConfigData 列表为空，路径: " + configDataPath);
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("更新文件数量失败");
        }
    }





}
