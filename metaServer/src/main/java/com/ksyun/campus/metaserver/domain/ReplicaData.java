package com.ksyun.campus.metaserver.domain;

public class ReplicaData {
    public String id;    // 副本的唯一标识符
    public String dsNode; // 数据存储节点的标识
    public String path;   // 副本在存储节点上的路径


    public ReplicaData(String id, String dsNode, String path) {
        this.id = id;
        this.dsNode = dsNode;
        this.path = path;
    }
    public ReplicaData() {}

    public String getId() {
        return id;
    }
    public void setId(String id) {
        this.id = id;
    }
    public String getDsNode() {
        return dsNode;
    }
    public void setDsNode(String dsNode) {
        this.dsNode = dsNode;
    }
    public String getPath() {
        return path;
    }
    public void setPath(String path) {
        this.path = path;
    }
}
