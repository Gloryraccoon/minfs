package com.ksyun.campus.dataserver.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ksyun.campus.dataserver.services.DataServerMsg;
import com.ksyun.campus.dataserver.services.RegistService;
import com.ksyun.campus.dataserver.utils.ConfigData;
import com.ksyun.campus.dataserver.utils.ReadResponse;
import com.ksyun.campus.dataserver.utils.StatInfo;
import org.apache.commons.io.FileUtils;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.io.entity.ByteArrayEntity;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;

import javax.servlet.http.HttpSession;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.*;

@RestController("/")
public class DataController {

    //数据块大小为64K
    private static final int BUFFER_SIZE = 64 * 1024;

    private final CloseableHttpClient httpClient = HttpClients.createDefault();

    @Value("${server.ip}")
    private String ip;

    @Value("${server.port}")
    private int port;

    @Value("${az.zone}")
    private String zone;

    @Value("${az.rack}")
    private String rack;

    @Autowired
    private RegistService registService;

    private String GetMetaServerUrl(String blockPath){

        ZooKeeper zk = registService.getZk();
        ObjectMapper objectMapper = new ObjectMapper();

        try {
            Stat stat = new Stat();
            byte[] data = zk.getData("/metaServers/leader", false, stat);

            // 将字节数组转换为字符串
            String jsonData = new String(data);

            // 解析 JSON 数据
            Map<String, String> leaderInfo = objectMapper.readValue(jsonData, Map.class);
            String ip = leaderInfo.get("ip");
            String port = leaderInfo.get("port");

            System.out.println("Leader IP: " + ip);
            System.out.println("Leader Port: " + port);

            return "http://" + ip + ":" + port + "/write?path=" + blockPath;
        }catch(Exception e){
            e.printStackTrace();
            System.out.println("获取leader信息失败");
            return null;
        }
    }

    @RequestMapping(value = "recoveryfile", method = RequestMethod.POST)
    public ResponseEntity<String> recoveryFile(@RequestHeader("fileSystemName") String fileSystemName,
                                               @RequestParam String path,
                                               @RequestBody List<ConfigData> configDataList) {
        int offset = 0;
        int totalBlock =0;
        while(true){
            byte[] buffer = new byte[BUFFER_SIZE];
            String blockName = String.valueOf(offset);
            String localPath = System.getProperty("user.dir") + "/Data/" + fileSystemName + path + "/" + blockName;
            File file = new File(localPath);
            if(!file.exists() && totalBlock == 0){
                System.out.println("本服务器无此文件");
                return new ResponseEntity<String>("本服务器不存在此文件",HttpStatus.NOT_FOUND);
            }else if(!file.exists()){
                break;
            }
            try {
                buffer = FileUtils.readFileToByteArray(file);
            }catch (Exception e){
                e.printStackTrace();
                return new ResponseEntity<String>("复制块"+offset+"失败", HttpStatus.BAD_REQUEST);
            }
            for(ConfigData configData : configDataList){
                String ip = configData.getIp();
                int port = configData.getPort();
                String requestUrl = "http://" + ip + ":" + port + "/write?path=" + path + "&offset=" + offset + "&length=" + buffer.length;
                //创建Post请求,附带缓冲区数据
                HttpPost post = new HttpPost(requestUrl);

                post.addHeader("fileSystemName", fileSystemName);
                post.setEntity(new ByteArrayEntity(buffer, 0,
                        buffer.length, ContentType.APPLICATION_OCTET_STREAM));
                //保证必须发送成功--暂时这么写
                while (true) {
                    try {
                        CloseableHttpResponse response = httpClient.execute(post);
                        int statusCode = response.getCode();
                        if (statusCode != 200) {
                            System.out.println("发送package" + offset + "失败");
                        } else {
                            System.out.println("发送package" + offset + "成功");
                            break;
                        }
                    }catch (Exception e){
                        System.out.println("文件发送失败");
                        e.printStackTrace();
                    }
                }
            }
            offset += BUFFER_SIZE;
            totalBlock++;
        }

        return new ResponseEntity<>("文件复制成功", HttpStatus.OK);
    }

    @RequestMapping(value = "recovery", method = RequestMethod.POST)
    public ResponseEntity<String> recoveryFile(@RequestHeader("fileSystemName") String fileSystemName,
                                                            @RequestParam String path,
                                                            @RequestParam int offset,
                                                            @RequestParam int length,
                                                            @RequestBody List<ConfigData> configDataList) {
        byte[] buffer = new byte[BUFFER_SIZE];
        String blockName = String.valueOf(offset);

        String LocalPath = System.getProperty("user.dir") + "/Data/" + fileSystemName + path + "/" + blockName;
        File file = new File(LocalPath);

        if(!file.exists()){
            System.out.println("该服务器不存在该文件");
            return new ResponseEntity<String>("该服务器不存在该文件", HttpStatus.NOT_FOUND);
        }

        try {
            buffer = FileUtils.readFileToByteArray(file);
        }catch (Exception e){
            e.printStackTrace();
            return new ResponseEntity<String>("该服务器拷贝文件失败", HttpStatus.BAD_REQUEST);
        }

        for(int i = 0;i < configDataList.size();i++){
            String dataip = configDataList.get(i).getIp();
            int dataport = configDataList.get(i).getPort();

            String requestUrl = "http://" + dataip + ":" + dataport + "/write?path=" + path + "&offset=" + offset + "&length=" + length;
            //创建Post请求,附带缓冲区数据
            HttpPost post = new HttpPost(requestUrl);

            post.addHeader("fileSystemName", fileSystemName);
            post.setEntity(new ByteArrayEntity(buffer, 0, length, ContentType.APPLICATION_OCTET_STREAM));

            //保证必须发送成功--暂时这么写
            while (true) {
                try {
                    CloseableHttpResponse response = httpClient.execute(post);
                    int statusCode = response.getCode();
                    if (statusCode != 200) {
                        System.out.println("发送package" + offset + "失败");
                    } else {
                        System.out.println("发送package" + offset + "成功");
                        break;
                    }
                }catch (Exception e){
                    System.out.println("文件发送失败");
                    e.printStackTrace();
                }
            }
        }
        return new ResponseEntity<>("拷贝成功", HttpStatus.OK);
    }



    /**
     * 1、读取request content内容并保存在本地磁盘下的文件内
     * 2、同步调用其他ds服务的write，完成另外2副本的写入
     * 3、返回写成功的结果及三副本的位置
     * @param fileSystemName
     * @param path
     * @param offset
     * @param length
     * @return
     */
    @RequestMapping("write")
    public synchronized ResponseEntity writeFile(@RequestHeader("fileSystemName") String fileSystemName,
                                    @RequestParam("path") String path,
                                    @RequestParam("offset") int offset,
                                    @RequestParam("length") int length,
                                    @RequestBody byte[] content){
        //todo 写本地
        //todo 调用远程ds服务写接口，同步副本，以达到多副本数量要求
        //todo 选择策略，按照 az rack->zone 的方式选取，将三副本均分到不同的az下
        //todo 支持重试机制
        //todo 返回三副本位置
        try {
            //获取项目根目录
            File rootDir = new File(System.getProperty("user.dir"));

            //创建Data目录(存储文件)
            File dataDir = new File(rootDir, "Data");
            if (!dataDir.exists()) {
                dataDir.mkdirs();
            }

            //检查或创建fileSystemName目录
            File fsDir = new File(dataDir, fileSystemName);
            if (!fsDir.exists()) {
                fsDir.mkdirs();
            }

            //解析path路径
            String[] pathParts = path.split("/");
            File currentDir = fsDir;

            for (int i = 1; i < pathParts.length - 1; i++) {
                currentDir = new File(currentDir, pathParts[i]);
                if (!currentDir.exists()) {
                    currentDir.mkdirs();
                }
            }

            //最后一个是文件名
            String fileName = pathParts[pathParts.length - 1];
            File fileFolder = new File(currentDir, fileName);
            if (!fileFolder.exists()) {
                fileFolder.mkdir(); // 创建文件夹
            }

            //创建普通文件并写入内容
            File outputFile = new File(fileFolder, String.valueOf(offset));
            try (FileOutputStream fos = new FileOutputStream(outputFile)) {
                fos.write(content, 0, length);
            }


            //向metaServer报告块的写入
            while(true) {

                RestTemplate restTemplate = new RestTemplate();
                HttpHeaders headers = new HttpHeaders();

                headers.set("fileSystemName", fileSystemName);

                String blockPath = path + "/" + offset;



                String MetaServerUrl = GetMetaServerUrl(blockPath);
                if(MetaServerUrl == null){
                    System.out.println("获取MetaServerUrl失败");
                    continue;
                }

                MetaServerUrl += "&offset=" + offset;
                MetaServerUrl += "&length=" + length;

                ConfigData configData = new ConfigData();
                configData.setIp(ip);
                configData.setPort(port);
                HttpEntity<ConfigData> requestEntity = new HttpEntity<>(configData, headers);

                ResponseEntity<String> response = restTemplate.exchange(MetaServerUrl, HttpMethod.POST, requestEntity, String.class);

                // 打印响应信息
                System.out.println(response.getBody());

                if(response.getStatusCode() == HttpStatus.OK){
                    break;
                }else{
                    System.out.println("commitWrite失败,重新向MetaServer提交");
                }
            }

            return new ResponseEntity(HttpStatus.OK);
        }catch (Exception e){
            System.out.println("DataServer写失败");
            e.printStackTrace();
            return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * 在指定本地磁盘路径下，读取指定大小的内容后返回
     * @param fileSystemName
     * @param path
     * @param offset
     * @param length
     * @return
     */
    @RequestMapping("read")
    public ResponseEntity<ReadResponse> readFile(@RequestHeader String fileSystemName, @RequestParam String path, @RequestParam int offset, @RequestParam int length){
        //todo 根据path读取指定大小的内容
        try{

            // 获取项目根目录
            File rootDir = new File(System.getProperty("user.dir"));

            // Data 目录(存储文件)
            File dataDir = new File(rootDir, "Data");
            if (!dataDir.exists()) {
                return new ResponseEntity<>(null, HttpStatus.NOT_FOUND);
            }

            // FileSystemName 目录
            File fsDir = new File(dataDir, fileSystemName);
            if (!fsDir.exists()) {
                return new ResponseEntity<>(null, HttpStatus.NOT_FOUND);
            }

            //解析path路径
            String[] pathParts = path.split("/");
            File currentDir = fsDir;

            for (int i = 1; i < pathParts.length - 1; i++) {
                currentDir = new File(currentDir, pathParts[i]);
                if (!currentDir.exists()) {
                    return new ResponseEntity<>(null, HttpStatus.NOT_FOUND);
                }
            }

            //最后一个是文件名
            String fileName = pathParts[pathParts.length - 1];
            File fileFolder = new File(currentDir, fileName);
            if (!fileFolder.exists()) {
                return new ResponseEntity<>(null, HttpStatus.NOT_FOUND);
            }

            //在最后一个目录中查找
            int tmp_offset = offset - offset % BUFFER_SIZE;

            String blockName = String.valueOf(tmp_offset);

            //块不存在
            File BlockFile = new File(fileFolder, blockName);

            if (!BlockFile.exists()) {

                System.out.println("请求恢复块");

                //找不到块，则请求同步
                ZooKeeper zk = registService.getZk();
                byte[] data = zk.getData("/metaServers/leader", false, null);

                String filemetadatapath = "/metadata/" + fileSystemName + path + "/" + blockName;

                //查询该块的大小
                byte[] filedata = zk.getData(filemetadatapath, false, null);
                if(filedata == null){
                    System.out.println("缺失块元数据无法找到");
                    return null;
                }

                System.out.println(filedata);

                // 解析元数据信息
                ObjectMapper mapper = new ObjectMapper();
                StatInfo statInfo = mapper.readValue(filedata, StatInfo.class);

                int fileSize = (int)statInfo.getSize();

                RestTemplate restTemplate = new RestTemplate();

                //转换成字符串
                String masterInfo = new String(data, StandardCharsets.UTF_8);
                mapper = new ObjectMapper();

                //解析json字符串
                JsonNode jsonNode = mapper.readTree(masterInfo);

                //获取master节点ip和port
                String metaip = jsonNode.get("ip").asText();
                String metaport = jsonNode.get("port").asText();

                String MetaRequestUrl = "http://" + metaip + ":" + metaport + "/blockRecovery?path=" + path;
                ConfigData configData = new ConfigData();
                configData.setIp(ip);
                configData.setPort(port);
                configData.setZone(zone);
                configData.setRack(rack);
                HttpHeaders headers = new HttpHeaders();
                headers.set("fileSystemName", fileSystemName);
                headers.setContentType(MediaType.APPLICATION_JSON);
                HttpEntity<ConfigData> requestEntity = new HttpEntity<>(configData, headers);

                ParameterizedTypeReference<List<ConfigData>> type = new ParameterizedTypeReference<List<ConfigData>>() {};
                // 发送 POST 请求
                ResponseEntity<List<ConfigData>> response = restTemplate.exchange(MetaRequestUrl, HttpMethod.POST, requestEntity,
                        type);

                // 处理响应
                List<ConfigData> ConfigList = response.getBody();

                List<ConfigData> LocalDataServer = new ArrayList<ConfigData>();
                LocalDataServer.add(new ConfigData(ip, port));

                int count = 0;
                for(int i = 0;i < ConfigList.size();i++){
                    String dataIp = ConfigList.get(i).getIp();
                    int dataPort = ConfigList.get(i).getPort();

                    String DataRequestUrl = "http://" + dataIp + ":" + dataPort + "/recovery?path=" + path + "&offset=" + blockName + "&length=" + fileSize;

                    restTemplate = new RestTemplate();
                    headers = new HttpHeaders();

                    headers.set("fileSystemName", fileSystemName);
                    headers.setContentType(MediaType.APPLICATION_JSON);

                    HttpEntity<List<ConfigData>> DataEntity = new HttpEntity<>(LocalDataServer, headers);

                    ResponseEntity<String> dataResponse = restTemplate.exchange(DataRequestUrl,  HttpMethod.POST, DataEntity, String.class);

                    if(dataResponse.getStatusCode() == HttpStatus.OK){
                        System.out.println("同步成功");
                        break;
                    }else{
                        System.out.println("同步失败");
                        ++count;
                    }
                }

                //全部失败,删除该文件
                if(count == ConfigList.size()){
                    MetaRequestUrl = "http://" + metaip + ":" + metaport + "/open?path=" + path;
                    restTemplate = new RestTemplate();
                    headers = new HttpHeaders();
                    headers.set("fileSystemName", fileSystemName);
                    HttpEntity<String> entity = new HttpEntity<>(headers);
                    // 获得DataServer列表
                    ParameterizedTypeReference<List<DataServerMsg>> typeRef = new ParameterizedTypeReference<List<DataServerMsg>>() {};
                    ResponseEntity<List<DataServerMsg>> delresponse = restTemplate.exchange(MetaRequestUrl, HttpMethod.POST, entity, typeRef);
                    List<DataServerMsg> deleteinfo = delresponse.getBody();
                    if (deleteinfo == null) {
                        System.out.println("未找到DataServer信息");
                    }

                    //依次调用每个 DataServer 的删除接口
                    for (DataServerMsg dataServerMsg : deleteinfo) {
                        String dataDeleteUrl = "http://" + dataServerMsg.getHost() + ":" + dataServerMsg.getPort() + "/delete?path=" + path;
                        ResponseEntity<String> dataDeleteResponse = restTemplate.exchange(dataDeleteUrl, HttpMethod.DELETE, entity, String.class);
                        if (dataDeleteResponse.getStatusCode() != HttpStatus.OK) {
                            System.out.println("DataServer 删除操作失败: " + dataDeleteResponse.getStatusCode());
                        }
                    }

                    MetaRequestUrl = "http://" + metaip + ":" + metaport + "/delete?path=" + path;
                    ResponseEntity<String> metaDeleteResponse = restTemplate.exchange(MetaRequestUrl, HttpMethod.DELETE, entity, String.class);
                    if (metaDeleteResponse.getStatusCode() != HttpStatus.OK) {
                        System.out.println("MetaServer 元数据删除操作失败: " + metaDeleteResponse.getBody());

                    }
                }

                return new ResponseEntity<>(null, HttpStatus.NOT_FOUND);
            }

            //读取文件内容
            byte[] buffer = new byte[length];
            try (FileInputStream fis = new FileInputStream(BlockFile)) {
                int bytesRead = fis.read(buffer, 0, length);
                if (bytesRead == -1) {
                    return new ResponseEntity<>(null, HttpStatus.NO_CONTENT); // 读取失败
                }

                ZooKeeper zk = registService.getZk();
                String nodePath = "/metadata/" + fileSystemName + path + "/" + blockName;
                byte[] nodeDataBytes = zk.getData(nodePath, false, null);
                String nodeData = new String(nodeDataBytes);

                // 解析 JSON 数据
                JSONObject jsonObject = new JSONObject(nodeData);
                int size = jsonObject.getInt("size");

                return new ResponseEntity<>(new ReadResponse(buffer, size), HttpStatus.OK);
            }
        }catch(Exception e){
            e.printStackTrace();
            System.out.printf("DataServer 读取失败: %s%n", e.getMessage());
            return new ResponseEntity<>(null, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * 删除指定路径的文件或目录
     * @param fileSystemName
     * @param path
     * @return
     */
    @RequestMapping("delete")
    public synchronized ResponseEntity deleteFile(@RequestHeader("fileSystemName") String fileSystemName,
                                                  @RequestParam("path") String path) {

        try {
            // 获取项目根目录
            File rootDir = new File(System.getProperty("user.dir"));

            // Data 目录(存储文件)
            File dataDir = new File(rootDir, "Data");

            // FileSystemName 目录
            File fsDir = new File(dataDir, fileSystemName);

            // 解析 path 路径
            String[] pathParts = path.split("/");
            File currentDir = fsDir;

            for (int i = 1; i < pathParts.length; i++) {
                currentDir = new File(currentDir, pathParts[i]);
            }

            // 检查文件或目录是否存在
            if (!currentDir.exists()) {
                return new ResponseEntity<>(HttpStatus.NOT_FOUND);
            }

            // 递归删除文件或目录
            deleteRecursively(currentDir);

            return new ResponseEntity<>(HttpStatus.OK);
        } catch (Exception e) {
            System.out.printf("DataServer 删除失败: %s%n", e.getMessage());
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * 递归删除目录及其内容，或删除单个文件
     * @param file
     */
    private void deleteRecursively(File file) {
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            if (files != null) {
                for (File subFile : files) {
                    deleteRecursively(subFile);
                }
            }
        }
        file.delete();
    }

    /**
     * 关闭退出进程
     */
    @RequestMapping("shutdown")
    public void shutdownServer(){
        System.exit(-1);
    }
}
