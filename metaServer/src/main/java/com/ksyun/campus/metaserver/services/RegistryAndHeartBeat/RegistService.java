package com.ksyun.campus.metaserver.services.RegistryAndHeartBeat;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Component
public class RegistService implements ApplicationRunner {

    private final HeartbeatService heartbeatService;
    private ScheduledExecutorService scheduler;

    /**
     * RegistService 构造函数。
     * 初始化时注入 HeartbeatService 实例。
     *
     * @param heartbeatService HeartbeatService 实例，负责与 Zookeeper 交互。
     */
    public RegistService(HeartbeatService heartbeatService) {
        this.heartbeatService = heartbeatService;
    }

    /**
     * 应用启动时执行的逻辑。
     * 注册节点到 Zookeeper，并调度心跳任务。
     *
     * @param args 应用启动时传递的参数。
     */
    @Override
    public void run(ApplicationArguments args) {
        try {
            // 注册节点
            heartbeatService.registerNode();

            // 调度心跳任务，每隔30秒发送一次心跳
            scheduleHeartbeatTask();

        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("failed to register node");
        }
    }

    /**
     * 调度心跳任务。
     * 使用 ScheduledExecutorService 每隔30秒执行一次心跳任务。
     */
    private void scheduleHeartbeatTask() {
        Runnable heartbeatTask = () -> {
            try {
                heartbeatService.sendHeartbeat();
            } catch (Exception e) {
                e.printStackTrace();
                System.err.println("Failed to heartbeat");
            }
        };

        scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(heartbeatTask, 0, 30, TimeUnit.SECONDS);
    }

    /**
     * 停止心跳任务。
     * 停止 ScheduledExecutorService。
     */
    @PreDestroy
    public void stopHeartbeat() {

        if (scheduler != null && !scheduler.isShutdown()) {
            scheduler.shutdown();
            try {
                scheduler.awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
//            System.out.println("Heartbeat scheduler stopped.");
        }

    }
}