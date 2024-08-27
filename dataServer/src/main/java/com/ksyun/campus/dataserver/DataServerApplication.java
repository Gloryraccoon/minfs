package com.ksyun.campus.dataserver;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class DataServerApplication {
    public static void main(String[] args) {
        SpringApplication.run(DataServerApplication.class,args);
    }
}
