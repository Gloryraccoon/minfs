package com.ksyun.campus.dataserver.services;

public class DataServerMsg {
    private String host;
    private int port;
    private String zone;
    private String rack;

    private int fileTotal;
    private int capacity;
    private int useCapacity;

    public DataServerMsg() {
    }

    public DataServerMsg(String host, int port, String zone, String rack, int fileTotal, int capacity, int useCapacity) {
        this.host = host;
        this.port = port;
        this.zone = zone;
        this.rack = rack;
        this.fileTotal = fileTotal;
        this.capacity = capacity;
        this.useCapacity = useCapacity;
    }

    // 缺省参数的构造函数
    public DataServerMsg(String host, int port, String zone, String rack) {
        this(host, port, zone, rack, 0, 0, 0);  // 默认 fileTotal 为 0，capacity 为 1000，useCapacity 为 0
    }

    public int getFileTotal() {
        return fileTotal;
    }

    public void setFileTotal(int fileTotal) {
        this.fileTotal = fileTotal;
    }

    public int getCapacity() {
        return capacity;
    }

    public void setCapacity(int capacity) {
        this.capacity = capacity;
    }

    public int getUseCapacity() {
        return useCapacity;
    }

    public void setUseCapacity(int useCapacity) {
        this.useCapacity = useCapacity;
    }

    public void setHost(String ip) {
        this.host = ip;
    }
    public String getHost() {
        return host;
    }
    public void setPort(int port) {
        this.port = port;
    }
    public int getPort() {
        return port;
    }
    public void setZone(String zone) {
        this.zone = zone;
    }
    public String getZone() {
        return zone;
    }
    public void setRack(String rack) {
        this.rack = rack;
    }
    public String getRack() {
        return rack;
    }
}
