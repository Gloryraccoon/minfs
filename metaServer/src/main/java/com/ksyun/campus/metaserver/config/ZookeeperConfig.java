package com.ksyun.campus.metaserver.config;

import com.ksyun.campus.metaserver.services.ZookeeperService;
import org.apache.curator.framework.CuratorFramework;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ZookeeperConfig {

    @Bean(destroyMethod = "close")
    public CuratorFramework curatorFramework(ZookeeperService zookeeperService) {
        return zookeeperService.getClient();
    }
}

