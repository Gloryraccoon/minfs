package com.ksyun.campus.dataserver.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ksyun.campus.dataserver.utils.ZooKeeperConnect;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

@Component
public class RegistService implements ApplicationRunner {

    @Autowired
    private ApplicationContext applicationContext;


    @Value("${server.ip}")
    private String ip;

    @Value("${server.port}")
    private int port;

    @Value("${server.ZooIp}")
    private String zooIp;

    @Value("${server.Zooport}")
    private int zooPort;

    @Value("${az.rack}")
    private String rack;

    @Value("${az.zone}")
    private String zone;

    private ZooKeeper zk;

    public ZooKeeper getZk() {
        return zk;
    }

    // 统计目录中的文件数量
    private int countFiles(File dir) {
        int count = 0;
        if (dir.isDirectory()) {
            for (File file : dir.listFiles()) {
                if (file.isDirectory()) {
                    count += countFiles(file);
                } else {
                    count++;
                }
            }
        }
        return count;
    }

    @Scheduled(initialDelay = 3000,fixedRate = 60000) // 每分钟执行一次
    public void reportServerStatus() {
        try {

            File dataDir = new File(System.getProperty("user.dir") + "/Data");
            int totalCapacity = 1024 * 1024 * 1024;
            int fileTotal = countFiles(dataDir);

            byte[] data = zk.getData("/dataservers/" + zone + "/" + rack + "/" + ip + ":" + port, false, null);

            ObjectMapper objectMapper = new ObjectMapper();

            DataServerMsg info = objectMapper.readValue(data, DataServerMsg.class);

            long usedCapacity = info.getUseCapacity();

            // 更新到 zk
            DataServerMsg updatedInfo = new DataServerMsg(ip, port, zone, rack, fileTotal, totalCapacity, (int) usedCapacity);

            objectMapper = new ObjectMapper();
            String json = objectMapper.writeValueAsString(updatedInfo);
            zk.setData("/dataservers/" + zone + "/" + rack + "/" + ip + ":" + port, json.getBytes(StandardCharsets.UTF_8), -1);

            System.out.println("定期更新 DataServer 状态至 ZooKeeper: " + updatedInfo.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //关闭该服务
    private void shutdownApplication() {
        if (applicationContext instanceof ConfigurableApplicationContext){
            ConfigurableApplicationContext ctx = (ConfigurableApplicationContext) applicationContext;
            SpringApplication.exit(ctx, () -> 1);
        }
        System.exit(1);
    }


    @Override
    public void run(ApplicationArguments args) throws Exception {
        registToCenter();
    }


    private void createPathRecursively(ZooKeeper zk, String path) throws KeeperException, InterruptedException {
        String[] parts = path.split("/");
        StringBuilder currentPath = new StringBuilder();

        for (String part : parts) {
            if (part.isEmpty()) continue;

            currentPath.append("/").append(part);
            Stat stat = zk.exists(currentPath.toString(), false);
            if (stat == null) {
                zk.create(currentPath.toString(), null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        }
    }

    public void registToCenter() {
        // todo 将本实例信息注册至zk中心，包含信息 ip、port、capacity、rack、zone
        String targethost = zooIp + ":" + zooPort;
        String localhost = ip + ":" + port;
        String zkPath = "/dataservers/" + zone + "/" + rack + "/" + localhost;

        try {
            zk = ZooKeeperConnect.connect(targethost, 3000);
            if(zk == null){
                //关闭该服务
                System.out.println("zookeeper连接失败");
                System.out.println("关闭服务");
                shutdownApplication();
            }

            // 确保上级路径存在
            createPathRecursively(zk, "/dataservers/" + zone + "/" + rack);


            //检查路径是否存在
            Stat stat = zk.exists(zkPath, false);
            if(stat == null){
                // 创建临时 ZNode 路径
                String pathCreated = zk.create(zkPath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                System.out.println("创建ZNode路径: " + pathCreated);
            }

            // 将服务器信息存储到 ZNode
            ObjectMapper objectMapper = new ObjectMapper();
            DataServerMsg serverInfo = new DataServerMsg(ip, port, zone, rack);

            String json = objectMapper.writeValueAsString(serverInfo);
            zk.setData(zkPath, json.getBytes(StandardCharsets.UTF_8), -1);

            System.out.println("DataServer信息已注册至:> " + zkPath);

            // 注册关闭钩子
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    if (zk.exists(zkPath, false) != null) {
                        zk.delete(zkPath, -1);
                        System.out.println("ZNode路径已删除: " + zkPath);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    try {
                        zk.close();
                        System.out.println("ZooKeeper连接已关闭");
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }));

        }catch (Exception e){
            e.printStackTrace();
            shutdownApplication();
        }
    }

    public List<Map<String, Integer>> getDslist() {
        return null;
    }
}
