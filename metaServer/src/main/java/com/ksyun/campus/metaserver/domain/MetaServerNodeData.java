package com.ksyun.campus.metaserver.domain;

public class MetaServerNodeData {
    private String ip;
    private String port;


    public MetaServerNodeData() {}

    public MetaServerNodeData(String ip, String port) {
        this.ip = ip;
        this.port = port;

    }

    public void setIp(String ip) {
        this.ip = ip;
    }
    public String getIp() {
        return ip;
    }
    public void setPort(String port) {
        this.port = port;
    }
    public String getPort() {
        return port;
    }
}