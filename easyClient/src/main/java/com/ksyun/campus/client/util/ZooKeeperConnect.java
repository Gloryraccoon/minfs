package com.ksyun.campus.client.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ksyun.campus.client.domain.DataServerMsg;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

//封装ZooKeeper连接获取
public class ZooKeeperConnect {

    //获取所有DataServer信息
    public static List<DataServerMsg> getDslist(ZooKeeper zk) {
        List<DataServerMsg> dataServerList = new ArrayList<>();
        String basePath = "/dataservers";

        try {
            // 获取所有 zone 的路径
            List<String> zones = zk.getChildren(basePath, false);
            for (String zone : zones) {
                // 获取 zone 下所有 rack 的路径
                String zonePath = basePath + "/" + zone;
                List<String> racks = zk.getChildren(zonePath, false);
                for (String rack : racks) {
                    // 获取 rack 下所有 DataServer 的路径
                    String rackPath = zonePath + "/" + rack;
                    List<String> dataServers = zk.getChildren(rackPath, false);
                    for (String dataServer : dataServers) {
                        String dataServerPath = rackPath + "/" + dataServer;
                        Stat stat = zk.exists(dataServerPath, false);
                        if (stat != null) {
                            byte[] data = zk.getData(dataServerPath, false, stat);
                            if (data != null) {
                                String json = new String(data, StandardCharsets.UTF_8);
                                ObjectMapper objectMapper = new ObjectMapper();
                                DataServerMsg serverInfo = objectMapper.readValue(json, DataServerMsg.class);

                                dataServerList.add(serverInfo);
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return dataServerList;
    }


    public static ZooKeeper connect(String host, int sessionTimeout) {
        try {
            ZooKeeper zooKeeper = new ZooKeeper(host, sessionTimeout, null);

            // 等待连接成功
            int attempts = 0;
            while (zooKeeper.getState() != ZooKeeper.States.CONNECTED && attempts < 10) {
                Thread.sleep(500); // 等待500ms后再检查
                attempts++;
            }

            if (zooKeeper.getState() == ZooKeeper.States.CONNECTED) {
                System.out.println("zooKeeper连接成功");
                return zooKeeper;
            } else {
                System.out.println("zooKeeper连接超时");
                return null;
            }
        }catch(Exception e){
            System.out.println("zooKeeper连接失败");
            return null;
        }
    }

}
