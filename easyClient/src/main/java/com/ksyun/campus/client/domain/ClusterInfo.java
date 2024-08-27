package com.ksyun.campus.client.domain;

import java.util.List;

public class ClusterInfo {
    private MetaServerMsg masterMetaServer;
    private List<MetaServerMsg> slaveMetaServerList;
    private List<DataServerMsg> dataServer;

    public MetaServerMsg getMasterMetaServer() {
        return masterMetaServer;
    }

    public void setMasterMetaServer(MetaServerMsg masterMetaServer) {
        this.masterMetaServer = masterMetaServer;
    }

    public List<MetaServerMsg> getSlaveMetaServerList() {
        return slaveMetaServerList;
    }

    public void setSlaveMetaServerList(List<MetaServerMsg> slaveMetaServerList) {
        this.slaveMetaServerList = slaveMetaServerList;
    }

    public List<DataServerMsg> getDataServer() {
        return dataServer;
    }

    public void setDataServer(List<DataServerMsg> dataServer) {
        this.dataServer = dataServer;
    }

    @Override
    public String toString() {
        return "ClusterInfo{" +
                "masterMetaServer=" + masterMetaServer +
                ", slaveMetaServerList=" + slaveMetaServerList +
                ", dataServer=" + dataServer +
                '}';
    }
}
