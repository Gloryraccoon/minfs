package com.ksyun.campus.dataserver.utils;

public class ReplicaData {
    public String id;
    public String dsNode;//格式为ip:port
    public String path;

    @Override
    public String toString() {
        return "ReplicaData{" +
                "id='" + id + '\'' +
                ", dsNode='" + dsNode + '\'' +
                ", path='" + path + '\'' +
                '}';
    }
}
