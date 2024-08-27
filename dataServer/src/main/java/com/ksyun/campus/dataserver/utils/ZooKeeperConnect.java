package com.ksyun.campus.dataserver.utils;

import org.apache.zookeeper.ZooKeeper;

//封装ZooKeeper连接获取
public class ZooKeeperConnect {

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
