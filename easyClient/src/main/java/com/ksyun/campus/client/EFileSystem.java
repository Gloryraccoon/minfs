package com.ksyun.campus.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ksyun.campus.client.domain.*;
import com.ksyun.campus.client.util.ConfigData;
import com.ksyun.campus.client.util.ZooKeeperConnect;

import org.apache.zookeeper.ZooKeeper;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.*;
import org.springframework.web.client.RestTemplate;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class EFileSystem extends FileSystem{

    public EFileSystem() {
        this("default");
    }

    public EFileSystem(String fileSystemName) {
        this.defaultFileSystemName = fileSystemName;
    }


public boolean delete(String path) {
        ZooKeeper zk = null;
        try {
            // 1. 连接到 ZooKeeper
            zk = ZooKeeperConnect.connect("localhost:2181", 3000);
            if (zk == null) {
                System.out.println("ZooKeeper 连接失败");
                return false;
            }

            // 2. 从 ZooKeeper 获取 MetaServer 信息
            byte[] data = zk.getData("/metaServers/leader", false, null);
            String masterInfo = new String(data, StandardCharsets.UTF_8);

            ObjectMapper mapper = new ObjectMapper();
            JsonNode jsonNode = mapper.readTree(masterInfo);

            // 3. 获取 MetaServer 的 IP 和端口
            String ipAddress = jsonNode.get("ip").asText();
            String port = jsonNode.get("port").asText();

            // 4. 构建获取数据的 URL
            String openUrl = "http://" + ipAddress + ":" + port + "/open?path=" + path;

            // 5. 使用 RestTemplate 获取文件的元数据信息
            RestTemplate restTemplate = new RestTemplate();
            HttpHeaders headers = new HttpHeaders();
            headers.set("fileSystemName", defaultFileSystemName);
            HttpEntity<String> entity = new HttpEntity<>(headers);

            // 获得DataServer列表
            ParameterizedTypeReference<List<DataServerMsg>> typeRef = new ParameterizedTypeReference<List<DataServerMsg>>() {};
            ResponseEntity<List<DataServerMsg>> response = restTemplate.exchange(openUrl, HttpMethod.POST, entity, typeRef);
            List<DataServerMsg> deleteinfo = response.getBody();
            if(deleteinfo.size() == 0 && response.getStatusCode() == HttpStatus.OK){
                return true;
            }else if (deleteinfo.size() ==0) {
                System.out.println("未找到DataServer信息");
                return false;
            }

            // 7. 依次调用每个 DataServer 的删除接口
            for (DataServerMsg dataServerMsg : deleteinfo) {
                String dataDeleteUrl = "http://" + dataServerMsg.getHost()+":"+dataServerMsg.getPort() + "/delete?path=" + path;
                ResponseEntity<String> dataDeleteResponse = restTemplate.exchange(dataDeleteUrl, HttpMethod.DELETE, entity, String.class);
                if (dataDeleteResponse.getStatusCode() != HttpStatus.OK) {
                    System.out.println("DataServer 删除操作失败: " + dataDeleteResponse.getStatusCode());
                    return false;
                }
            }

            // 8. 删除 DataServer 上的数据后，删除 MetaServer 上的元数据
            String metaDeleteUrl = "http://" + ipAddress + ":" + port + "/delete?path=" + path;
            ResponseEntity<String> metaDeleteResponse = restTemplate.exchange(metaDeleteUrl, HttpMethod.DELETE, entity, String.class);
            if (metaDeleteResponse.getStatusCode() != HttpStatus.OK) {
                System.out.println("MetaServer 元数据删除操作失败: " + metaDeleteResponse.getBody());

                return false;
            }

            return true;

        } catch (Exception e) {
            System.out.println("删除操作发生异常：" + e.getMessage());
            return false;
        } finally {
            try {
                if (zk != null) {
                    zk.close();
                }
            } catch (Exception e) {
                System.out.println("ZooKeeper 客户端关闭异常");
            }
        }
    }

    //获取集群信息
    public ClusterInfo getClusterInfo(){
        ZooKeeper zk = null;
        ClusterInfo clusterInfo = new ClusterInfo();

        try{
            zk = ZooKeeperConnect.connect("localhost:2181", 3000);
            if (zk == null) {
                System.out.println("ZooKeeper 连接失败");
                return null;
            }

            //核心逻辑

            // 获取 leader 信息
            String leaderPath = "/metaServers/leader";
            byte[] leaderData = zk.getData(leaderPath, false, null);
            ConfigData leaderNode = new ObjectMapper().readValue(leaderData, ConfigData.class);

            MetaServerMsg metaServerMsg = new MetaServerMsg();
            metaServerMsg.setHost(leaderNode.getIp());
            metaServerMsg.setPort(leaderNode.getPort());
            //设置leader的信息
            clusterInfo.setMasterMetaServer(metaServerMsg);

            // 获取所有 metaServer 节点信息
            String metaServersPath = "/metaServers";

            List<String> children = zk.getChildren(metaServersPath, false);
            List<MetaServerMsg> followers = new ArrayList<>();

            for(String child : children){
                if(!child.equals("leader")){
                    String childPath = metaServersPath + "/" + child;
                    byte[] childData = zk.getData(childPath, false, null);
                    ConfigData metaServerNode = new ObjectMapper().readValue(childData, ConfigData.class);

                    // 排除 leader 的 IP 和 port
                    if (!metaServerNode.getIp().equals(leaderNode.getIp()) || !(metaServerNode.getPort() == leaderNode.getPort())) {
                        MetaServerMsg metaServerMsg2 = new MetaServerMsg();
                        metaServerMsg2.setHost(metaServerNode.getIp());
                        metaServerMsg2.setPort(metaServerNode.getPort());

                        followers.add(metaServerMsg2);
                    }
                }
            }

            clusterInfo.setSlaveMetaServerList(followers);

            //获取DataServer列表
            List<DataServerMsg> dataServers = ZooKeeperConnect.getDslist(zk);

            clusterInfo.setDataServer(dataServers);

            return clusterInfo;

        }catch(Exception e){
            System.out.println("获取集群信息失败: " + e.getMessage());
            return null;
        }finally{
            try {
                if (zk != null) {
                    zk.close();
                }
            } catch (Exception e) {
                System.out.println("ZooKeeper 客户端关闭异常");
            }
        }
    }

    public List<StatInfo> listFileStats(String path){
        List<StatInfo> fileStatsList = new ArrayList<>();
        ZooKeeper zk = null;
        try{
            // 连接到 ZooKeeper（或其他元数据存储系统）
            zk = ZooKeeperConnect.connect("localhost:2181", 3000);
            if (zk == null) {
                System.out.println("ZooKeeper 连接失败");
                return null;
            }

            // 构建查询路径
            String metadataPath = "/metadata/" + defaultFileSystemName + path;

            // 获取元数据信息
            byte[] data = zk.getData(metadataPath, false, null);
            if (data == null) {
                System.out.println("未找到元数据");
                return null;
            }

            // 解析元数据信息
            ObjectMapper mapper = new ObjectMapper();
            StatInfo statInfo = mapper.readValue(data, StatInfo.class);

            if(statInfo != null){
                if(statInfo.getType() != FileType.Directory){
                    System.out.printf("非目录不处理");
                    return null;
                }else{
                    // 获取指定目录下的子节点列表
                    List<String> children = zk.getChildren(metadataPath, false);
                    for (String child : children) {
                        // 获取每个子节点的元数据信息
                        String childPath = metadataPath + "/" + child;
                        byte[] childdata = zk.getData(childPath, false, null);

                        if (childdata != null) {
                            // 解析元数据信息
                            ObjectMapper childmapper = new ObjectMapper();
                            StatInfo childstatInfo = childmapper.readValue(childdata, StatInfo.class);
                            fileStatsList.add(childstatInfo);
                        }
                    }
                }
            }else{
                System.out.printf("元数据解析失败");
                return null;
            }
        }catch(Exception e){
            System.out.println("获取目录下文件元数据失败(可能不存在): " + e.getMessage());
            return null;
        }finally {
            try {
                if (zk != null) {
                    zk.close();
                }
            } catch (Exception e) {
                System.out.println("ZooKeeper 客户端关闭异常");
            }
        }

        return fileStatsList;
    }


    public StatInfo getFileStats(String path){
        ZooKeeper zk = null;
        try{
            // 连接到 ZooKeeper（或其他元数据存储系统）
            zk = ZooKeeperConnect.connect("localhost:2181", 3000);
            if (zk == null) {
                System.out.println("ZooKeeper 连接失败");
                return null;
            }

            // 构建查询路径
            String metadataPath = "/metadata/" + defaultFileSystemName + path;

            // 获取元数据信息
            byte[] data = zk.getData(metadataPath, false, null);
            if (data == null) {
                System.out.println("未找到元数据");
                return null;
            }

            // 解析元数据信息
            ObjectMapper mapper = new ObjectMapper();
            StatInfo statInfo = mapper.readValue(data, StatInfo.class);

            return statInfo;
        }catch(Exception e){
            System.out.println("获取元数据失败: " + e.getMessage());
            return null;
        }finally{
            try {
                if (zk != null) {
                    zk.close();
                }
            } catch (Exception e) {
                System.out.println("ZooKeeper 客户端关闭异常");
            }
        }
    }

    public boolean mkdir(String path) {
        ZooKeeper zk = null;
        try {
            // 1. 连接到 ZooKeeper 获取 MetaServer 信息
            zk = ZooKeeperConnect.connect("localhost:2181", 3000);
            if (zk == null) {
                System.out.println("ZooKeeper 连接失败");
                return false;
            }

            // 2. 从 ZooKeeper 获取 MetaServer 信息
            byte[] data = zk.getData("/metaServers/leader", false, null);
            String masterInfo = new String(data, StandardCharsets.UTF_8);

            ObjectMapper mapper = new ObjectMapper();
            JsonNode jsonNode = mapper.readTree(masterInfo);

            // 3. 获取 MetaServer 的 IP 和端口
            String ipAddress = jsonNode.get("ip").asText();
            String port = jsonNode.get("port").asText();

            // 4. 构建 mkdir 请求的 URL
            String mkdirUrl = "http://" + ipAddress + ":" + port + "/mkdir?path=" + path;

            // 5. 使用 RestTemplate 调用 MetaServer 的 mkdir 接口
            RestTemplate restTemplate = new RestTemplate();
            HttpHeaders headers = new HttpHeaders();
            headers.set("fileSystemName", defaultFileSystemName);
            HttpEntity<String> entity = new HttpEntity<>(headers);

            ResponseEntity<String> response = restTemplate.exchange(mkdirUrl, HttpMethod.POST, entity, String.class);
            System.out.println(response.getBody());
            // 6. 处理响应
            if (response.getStatusCode() == HttpStatus.OK) {
                System.out.println("目录创建成功: " + path);
                return true;
            } else {
                System.out.println("目录创建失败: " + response.getStatusCode());
                return false;
            }

        } catch (Exception e) {
            System.out.println("创建目录操作发生异常：" + e.getMessage());
            return false;
        } finally {
            try {
                if (zk != null) {
                    zk.close();
                }
            } catch (Exception e) {
                System.out.println("ZooKeeper 客户端关闭异常");
            }
        }
    }

    public FSOutputStream create(String path){
        ZooKeeper zk = null;
        try {
            zk = ZooKeeperConnect.connect("localhost:2181", 3000);
            if(zk == null){
                System.out.println("zookeeper连接失败");
                return null;
            }else{
                System.out.println("zookeeper连接成功");
            }

            byte[] data = zk.getData("/metaServers/leader", false, null);

            //转换成字符串
            String masterInfo = new String(data, StandardCharsets.UTF_8);

            ObjectMapper mapper = new ObjectMapper();

            //解析json字符串
            JsonNode jsonNode = mapper.readTree(masterInfo);

            //获取master节点ip和port
            String ipAddress = jsonNode.get("ip").asText();
            String port = jsonNode.get("port").asText();

            //构建MetaServer的create接口url
            String createUrl = "http://" + ipAddress + ":" + port + "/create?path=" + path;

            //使用RestTemplate调用MetaServer的create接口
            RestTemplate restTemplate = new RestTemplate();
            HttpHeaders headers = new HttpHeaders();
            headers.set("fileSystemName", defaultFileSystemName);
            HttpEntity<String> entity = new HttpEntity<>(headers);

            //获得DataServer列表
            ParameterizedTypeReference<List<DataServerMsg>> responseType = new ParameterizedTypeReference<List<DataServerMsg>>() {};
            ResponseEntity<List<DataServerMsg>> response = restTemplate.exchange(createUrl, HttpMethod.POST, entity, responseType);

            /*--接下来构造FSOutPutStream--*/
            // 解析出列表
            List<DataServerMsg> dataServerMsgList = response.getBody();

            //处理极端情况
            if(dataServerMsgList == null || dataServerMsgList.isEmpty()){
                return null;
            }

            List<String> HostList = new ArrayList<String>();
            List<Integer> PortList = new ArrayList<Integer>();

            for(int i = 0;i < dataServerMsgList.size();i++){
                HostList.add(dataServerMsgList.get(i).getHost());
                PortList.add(dataServerMsgList.get(i).getPort());
            }

            return new FSOutputStream(HostList, PortList, defaultFileSystemName, path);

        }catch(Exception e){
            e.printStackTrace();
            System.out.println("create功能发生异常");
            return null;
        }finally {
            try {
                if(zk != null) {
                    zk.close();
                }
            }catch(Exception e){
                System.out.println("zookeeper客户端关闭异常");
            }
        }
    }

    public FSInputStream open(String path){
        ZooKeeper zk = null;
        try{
            zk = ZooKeeperConnect.connect("localhost:2181", 3000);
            if(zk == null){
                System.out.println("zookeeper连接失败");
                return null;
            }else{
                System.out.println("zookeeper连接成功");
            }
            byte[] data = zk.getData("/metaServers/leader", false, null);

            //转换成字符串
            String masterInfo = new String(data, StandardCharsets.UTF_8);

            ObjectMapper mapper = new ObjectMapper();

            //解析json字符串
            JsonNode jsonNode = mapper.readTree(masterInfo);

            //获取master节点ip和port
            String ipAddress = jsonNode.get("ip").asText();
            String port = jsonNode.get("port").asText();

            //构建MetaServer的open接口url
            String openUrl = "http://" + ipAddress + ":" + port + "/open?path=" + path;

            // 使用RestTemplate调用MetaServer的open接口
            RestTemplate restTemplate = new RestTemplate();
            HttpHeaders headers = new HttpHeaders();
            headers.set("fileSystemName", defaultFileSystemName);
            HttpEntity<String> entity = new HttpEntity<>(headers);

            //获得DataServer列表
            ParameterizedTypeReference<List<DataServerMsg>> responseType = new ParameterizedTypeReference<List<DataServerMsg>>() {};
            ResponseEntity<List<DataServerMsg>> response = restTemplate.exchange(openUrl, HttpMethod.POST, entity, responseType);

            /*--接下来构造FSInputStream--*/
            // 解析出列表
            List<DataServerMsg> dataServerMsgList = response.getBody();

            //处理极端情况
            if(dataServerMsgList == null || dataServerMsgList.isEmpty()){
                return null;
            }

            List<String> HostList = new ArrayList<String>();
            List<Integer> PortList = new ArrayList<Integer>();

            for(int i = 0;i < dataServerMsgList.size();i++){
                HostList.add(dataServerMsgList.get(i).getHost());
                PortList.add(dataServerMsgList.get(i).getPort());
            }

            return new FSInputStream(HostList, PortList, defaultFileSystemName, path);

        }catch(Exception e){
            System.out.println("open功能发生异常");
            return null;
        }finally{
            try {
                if(zk != null) {
                    zk.close();
                }
            } catch(Exception e){
                System.out.println("zookeeper客户端关闭异常");
            }
        }
    }


}
