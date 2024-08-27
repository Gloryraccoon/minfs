package com.ksyun.campus.metaserver.domain;

public class DataServerMsg {
    private String host;
    private int port;
    private String zone;
    private String rack;
    private int fileTotal;//存储的文件总数
    private int capacity;//总存储容量
    private int useCapacity;//已使用的存储容量

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
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

    @Override
    public String toString() {
        return "DataServerMsg{" +
                "host='" + host + '\'' +
                ", port=" + port +
                ", fileTotal=" + fileTotal +
                ", capacity=" + capacity +
                ", useCapacity=" + useCapacity +
                '}';
    }
}
