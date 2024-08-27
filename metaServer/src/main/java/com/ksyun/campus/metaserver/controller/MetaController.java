package com.ksyun.campus.metaserver.controller;


import com.ksyun.campus.metaserver.domain.*;
import com.ksyun.campus.metaserver.services.MetaServerLeaderElection;
import com.ksyun.campus.metaserver.services.MetaService;
import com.ksyun.campus.metaserver.services.ZookeeperService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/")
public class MetaController {

    @Autowired
    private MetaService metaService;
    @Autowired
    private ZookeeperService zookeeperService;
    @Autowired
    private MetaServerLeaderElection leaderElection;

    // 查询某个路径下的文件或目录的元数据信息
    @RequestMapping("stats")
    public ResponseEntity<StatInfo> stats(@RequestHeader String fileSystemName, @RequestParam String path) {
        try {
            if (metaService.isMetadataExists(fileSystemName+path)) {
                StatInfo statInfo = zookeeperService.getNodeData("/metadata/" + fileSystemName + path);
                return new ResponseEntity<>(statInfo, HttpStatus.OK);
            } else {
                return new ResponseEntity<>(HttpStatus.NOT_FOUND);
            }
        } catch (Exception e) {
            System.out.println("查询元数据失败: " + e.getMessage());
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    // 创建一个新文件
    @RequestMapping("create")
    public ResponseEntity<List<DataServerMsg>> createFile(@RequestHeader String fileSystemName, @RequestParam String path) {
        if (!leaderElection.isLeader()) {
            return new ResponseEntity<>(null, HttpStatus.FORBIDDEN);
        }
        try {
            if (metaService.isParentPathExists(fileSystemName+path)) {
                // 调用 pickBalancedSlaveDataServers 方法
                List<DataServerMsg> dataServers = metaService.selectServerWithWeight();

                String nodePath="/metadata/" + fileSystemName+path;

                if (dataServers != null && !dataServers.isEmpty()) {

                    //注册元数据
                    StatInfo statInfo = new StatInfo();
                    statInfo.setPath(path);
                    statInfo.setSize(0); // 初始文件大小为 0
                    statInfo.setMtime(System.currentTimeMillis()); // 设置修改时间
                    statInfo.setType(FileType.File); // 设置为文件类型
                    List<ReplicaData> replicaDataList = new ArrayList<>();
                    for (DataServerMsg server : dataServers) {

                        ReplicaData replicaData = new ReplicaData();
                        replicaData.setId(UUID.randomUUID().toString());
                        replicaData.setDsNode("/"+server.getHost()+"/"+server.getPort()+"/"+server.getZone()+"/"+server.getRack());
                        replicaData.setPath(path);
                        replicaDataList.add(replicaData);
                        }
                    statInfo.setReplicaData(replicaDataList);
                    // 在 Zookeeper 中创建路径节点
                    zookeeperService.registerNode(nodePath, statInfo);


                    // 将 DataServerMsg 转换为 ConfigData
                    List<ConfigData> configDataList = new ArrayList<>();
                    for (DataServerMsg server : dataServers) {


                        //注册副本数据
                        ConfigData CreateConfigData = new ConfigData();
                        CreateConfigData.setIp(server.getHost());
                        CreateConfigData.setPort(server.getPort());
                        CreateConfigData.setZone(server.getZone());
                        CreateConfigData.setRack(server.getRack());
                        configDataList.add(CreateConfigData);
                    }
                    // 使用新的 registerNode 方法在 Zookeeper 中创建 config 节点并存储 JSON 数据
                    statInfo.setReplicaData(replicaDataList);
                    zookeeperService.registerNode(nodePath + "/config", configDataList);

                    return new ResponseEntity<>(dataServers, HttpStatus.OK);
                } else {
                    return new ResponseEntity<>(null, HttpStatus.INTERNAL_SERVER_ERROR);
                }
            } else {
                return new ResponseEntity<>(null, HttpStatus.FORBIDDEN);
            }

        } catch (Exception e) {
            System.out.println("创建文件失败: " + e.getMessage());
            return new ResponseEntity<>(null, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    // 创建一个新目录
    @RequestMapping("mkdir")
    public ResponseEntity<String> mkdir(@RequestHeader String fileSystemName, @RequestParam String path) {
        if (!leaderElection.isLeader()) {
            return new ResponseEntity<>(null, HttpStatus.FORBIDDEN);
        }
        try {
            if (metaService.isParentPathExists(fileSystemName+path)) {

                    String nodePath="/metadata/" + fileSystemName + path;

                    StatInfo statInfo = new StatInfo();
                    statInfo.setPath(path);
                    statInfo.setSize(0); // 初始文件大小为 0
                    statInfo.setMtime(System.currentTimeMillis()); // 设置修改时间
                    statInfo.setType(FileType.Directory); // 设置为文件类型
                    statInfo.setReplicaData(null);
                    // 在 Zookeeper 中创建路径节点
                    zookeeperService.registerNode(nodePath, statInfo);

                    return new ResponseEntity<>("创建目录成功", HttpStatus.OK);
            } else {
                return new ResponseEntity<>("父目录不存在", HttpStatus.NOT_FOUND);
            }

        } catch (Exception e) {
            System.out.println("创建目录失败: " + e.getMessage());
            return new ResponseEntity<>("创建目录失败", HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    // 列出某个目录下的所有文件和子目录
    @RequestMapping("listdir")
    public ResponseEntity<List<String>> listdir(@RequestHeader String fileSystemName, @RequestParam String path) {
        try {
            List<String> children = zookeeperService.getChildren("/metadata/" + fileSystemName + path);
            return new ResponseEntity<>(children, HttpStatus.OK);
        } catch (Exception e) {
            System.out.println("列出文件失败: " + e.getMessage());
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    // 删除指定路径下的文件或目录
    @RequestMapping("delete")
    public ResponseEntity<String> delete(@RequestHeader String fileSystemName, @RequestParam String path) {
        if (!leaderElection.isLeader()) {
            return new ResponseEntity<>("从节点禁止删除操作", HttpStatus.FORBIDDEN);
        }

        try {
            // 检查路径是否存在
            if (metaService.isParentPathExists(fileSystemName + path)) {
                if (metaService.isMetadataExists(fileSystemName + path)) {
                    String nodePath = "/metadata/" + fileSystemName + path;

                    // 获取节点数据，准备递归更新父目录的大小
                    StatInfo statInfo = zookeeperService.getNodeData(nodePath);

//                    //更新dataServer的可用容量
//                    // 获取 /metadata/+nodePath+/config 下的 config 数据
//                    List<ConfigData> configDataList = zookeeperService.getConfigData(nodePath + "/config");

//                    if (configDataList != null) {
//                        // 调用 updateDataServerUseCapacity 方法更新 useCapacity
//                        for (ConfigData configData : configDataList) {
//                            String dataServerNodePath = "/dataservers/" + configData.getZone() + "/" + configData.getRack() + "/" + configData.getIp() + ":" + configData.getPort();
//                            // flag参数为 1 表示增加，-1 表示减少 useCapacity
//                            zookeeperService.updateDataServerUseCapacity(dataServerNodePath, (int)statInfo.getSize(), -1);
//                        }
//                    }

                    long sizeToRemove = (statInfo != null) ? statInfo.getSize() : 0;

                    // 递归更新父目录的大小
                    String currentPath = nodePath;
                    while (true) {
                        currentPath = currentPath.substring(0, currentPath.lastIndexOf('/'));
                        if (currentPath.equals("/metadata/" + fileSystemName)) {
                            break;
                        } else {
                            StatInfo parentStatInfo = zookeeperService.getNodeData(currentPath);

                            parentStatInfo.setSize(parentStatInfo.getSize() - sizeToRemove);
                            parentStatInfo.setMtime(System.currentTimeMillis());
                            zookeeperService.registerNode(currentPath, parentStatInfo);
                        }
                    }

                    // 检查节点是否有子节点并递归删除
                    if (zookeeperService.hasChildren(nodePath)) {
                        zookeeperService.deleteNodeRecursively(nodePath);
                    } else {
                        zookeeperService.deleteNode(nodePath);
                    }

                    return new ResponseEntity<>(HttpStatus.OK);
                } else {
                    return new ResponseEntity<>("路径文件不存在", HttpStatus.NOT_FOUND);
                }
            } else {
                return new ResponseEntity<>("父目录不存在", HttpStatus.NOT_FOUND);
            }
        } catch (Exception e) {
            System.out.println("删除失败: " + e.getMessage());
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }




    /**
     * 保存文件写入成功后的元数据信息，包括文件path、size、三副本信息等
     *
     * @param fileSystemName
     * @param path
     * @param offset
     * @param length
     * @return
     */
    @RequestMapping("write")
    public ResponseEntity<String> commitWrite(@RequestHeader String fileSystemName, @RequestParam String path,
                                              @RequestParam int offset, @RequestParam int length) {
        if (!leaderElection.isLeader()) {
            return new ResponseEntity<>(null, HttpStatus.FORBIDDEN);
        }
        try {
            String nodePath=fileSystemName + path;

            if (metaService.isParentPathExists(nodePath)) {
                // 设置标志位路径
                String flagPath = "/flagPath/" + nodePath;

                // 检查标志位是否已经设置
                if (!zookeeperService.nodeExists(flagPath)) {
                    // 逐级更新父目录的 size 和 mtime
                    String currentPath = "/metadata/" + nodePath;
                    while (true) {
                        currentPath = currentPath.substring(0, currentPath.lastIndexOf('/'));
                        if(currentPath.equals( "/metadata/"+fileSystemName)){
                            break;
                        }else{
                        StatInfo parentStatInfo = zookeeperService.getNodeData(currentPath);

                            parentStatInfo.setSize(parentStatInfo.getSize() + length);
                            parentStatInfo.setMtime(System.currentTimeMillis());
                            zookeeperService.registerNode(currentPath, parentStatInfo);
                        }
                    }

                    // 设置标志位，表示该文件路径已处理，并创建临时节点
                    zookeeperService.registerEphemeralNode(flagPath, "1");

                    //更新文件块的副本信息
                    StatInfo statInfo = new StatInfo();
                    statInfo.setPath(path);
                    statInfo.setSize(length); // 初始文件大小为 length
                    statInfo.setMtime(System.currentTimeMillis()); // 设置修改时间
                    statInfo.setType(FileType.Unknown); // 设置为文件类型\
                    statInfo.setReplicaData(null);

                    // 在 Zookeeper 中创建路径节点
                    zookeeperService.registerNode("/metadata/"+nodePath, statInfo);

//                    //更新dataServer的可用容量
//                    // 获取 /metadata/+nodePath+/config 下的 config 数据
//                    List<ConfigData> configDataList = zookeeperService.getConfigData("/metadata/" + nodePath + "/config");
//
//                    if (configDataList != null) {
//                        // 调用 updateDataServerUseCapacity 方法更新 useCapacity
//                        for (ConfigData configData : configDataList) {
//                            String dataServerNodePath = "/dataservers/" + configData.getZone() + "/" + configData.getRack() + "/" + configData.getIp() + ":" + configData.getPort();
//                            // flag参数为 1 表示增加，-1 表示减少 useCapacity
//                            zookeeperService.updateDataServerUseCapacity(dataServerNodePath, length, 1);
//                        }
//                    }

                    return new ResponseEntity<>("写文件成功", HttpStatus.OK);
                } else {
                    return new ResponseEntity<>("标志位已存在，文件已处理", HttpStatus.OK);
                }
            } else {
                return new ResponseEntity<>("父目录不存在", HttpStatus.NOT_FOUND);
            }
        } catch (Exception e) {
            System.out.println("写文件失败: " + e.getMessage());
            return new ResponseEntity<>(null, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }




    /**
     * 根据文件path查询三副本的位置，返回客户端具体ds、文件分块信息
     *
     * @param fileSystemName
     * @param path
     * @return
     */
    @RequestMapping("open")
    public ResponseEntity<List<DataServerMsg>> open(@RequestHeader String fileSystemName, @RequestParam String path) {
        String configDataPath = "/metadata/" + fileSystemName + path + "/config";
        String nodePath = "/metadata/" + fileSystemName + path;
        try {
            // 检查 config 子节点是否存在
            if (zookeeperService.nodeExists(configDataPath)) {

                // 获取配置数据
                List<ConfigData> configDataList = zookeeperService.getConfigData(configDataPath);

                // 将 ConfigData 转换为 DataServerMsg
                List<DataServerMsg> dataServerMsgList = configDataList.stream().map(config -> {
                    DataServerMsg msg = new DataServerMsg();
                    msg.setHost(config.getIp());
                    msg.setPort(config.getPort());
                    msg.setZone(config.getZone());
                    msg.setRack(config.getRack());

                    msg.setFileTotal(0);
                    msg.setCapacity(0);
                    msg.setUseCapacity(0);

                    return msg;
                }).collect(Collectors.toList());

                // 返回 DataServerMsg 列表
                return new ResponseEntity<>(dataServerMsgList, HttpStatus.OK);
            } else {
                zookeeperService.deleteNodeRecursively(nodePath);
                return new ResponseEntity<>(Collections.emptyList(), HttpStatus.OK);
            }
        } catch (Exception e) {
            // 处理异常并返回500
            return new ResponseEntity<>(Collections.emptyList(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }


    @RequestMapping("blockRecovery")
    public ResponseEntity<List<ConfigData>> blockRecovery(@RequestHeader String fileSystemName, @RequestParam String path,
                                                          @RequestBody ConfigData configData) {
        try {
            // 获取 configDataPath 路径
            String configDataPath = "/metadata/" + fileSystemName + path + "/config";

            // 获取存储在指定路径下的 ConfigData 列表
            List<ConfigData> configDataList = zookeeperService.getConfigData(configDataPath);

            if (configDataList != null && !configDataList.isEmpty()) {
                // 从 configDataList 中去除与请求体 configData 相同的数据
                configDataList.removeIf(config ->
                        config.getIp().equals(configData.getIp()) &&
                                config.getPort() == configData.getPort() &&
                                config.getZone().equals(configData.getZone()) &&
                                config.getRack().equals(configData.getRack()));

                // 返回去除后的 configDataList
                return ResponseEntity.ok(configDataList);
            } else {
                // 如果没有找到数据，则返回 404 Not Found
                return ResponseEntity.status(HttpStatus.NOT_FOUND).body(null);
            }

        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(null);
        }
    }

    /**
     * 关闭退出进程
     */
    @RequestMapping("shutdown")
    public void shutdownServer() {
        System.exit(-1);
    }

}