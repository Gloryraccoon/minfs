package com.ksyun.campus.client.util;

public class ConfigData {
    private String ip;
    private int port;
    private String zone;
    private String rack;

    // 默认构造函数
    public ConfigData() {}

    // 全参构造函数
    public ConfigData(String ip, int port, String zone, String rack) {
        this.ip = ip;
        this.port = port;
        this.zone = zone;
        this.rack = rack;
    }

    // Getter 和 Setter 方法
    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }
    public String getZone() {
        return zone;
    }
    public void setZone(String zone) {
        this.zone = zone;
    }
    public String getRack() {
        return rack;
    }
    public void setRack(String rack) {
        this.rack = rack;
    }
}
