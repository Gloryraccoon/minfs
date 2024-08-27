package com.ksyun.campus.metaserver.domain;

import java.util.List;

public class StatInfo {
    private String path;
    private long size;
    private long mtime;
    private FileType type;
    private List<ReplicaData> replicaData;

    // 默认构造函数
    public StatInfo() {}

    // Getter 和 Setter 方法
    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public long getMtime() {
        return mtime;
    }

    public void setMtime(long mtime) {
        this.mtime = mtime;
    }

    public FileType getType() {
        return type;
    }

    public void setType(FileType type) {
        this.type = type;
    }

    public List<ReplicaData> getReplicaData() {
        return replicaData;
    }

    public void setReplicaData(List<ReplicaData> replicaData) {
        this.replicaData = replicaData;
    }
}
