package com.yudy.heze.config;

import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.util.Properties;

public class ServerConfig extends Config {

    public ServerConfig(Properties props) {
        super(props);
    }

    public ServerConfig(String filename) {
        super(filename);
    }

    public ServerConfig(File cfg) {
        super(cfg);
    }

    /**
     * 端口号
     */
    public int getPort() {
        return getInt("mq.port", 9092);
    }

    /**
     * Broker的ip地址
     */
    public String getHost() {
        return getString("mq.host", null);
    }


    public String getServerName(){return getString("mq.server",null);}

    /**
     * 备份节点
     *
     * @return
     */
    public String getReplicaHost() {
        return getString("mq.replica.host", null);
    }

    /**
     * ZK host string
     */
    public String getZkConnect() {
        return getString("mq.zk.connect", null);
    }

    public String getZkUsername() {
        return getString("mq.zk.username", "");
    }

    public String getZkPassword() {
        return getString("mq.zk.password", "");
    }


    public int getEachFetchRecordNums(){return getInt("mq.fetch.records",5);}

    /**
     * zookeeper session timeout
     */
    public int getZkSessionTimeoutMs() {
        return getInt("mq.zk.sessiontimeout.ms", 6000);
    }

    /**
     * the max time that the client waits to establish a connection to
     * zookeeper
     */
    public int getZkConnectionTimeoutMs() {
        return getInt("mq.zk.connectiontimeout.ms", 6000);
    }

    /**
     * how far a ZK follower can be behind a ZK leader
     */
    public int getZkSyncTimeMs() {
        return getInt("mq.zk.synctime.ms", 2000);
    }

    public String getDataDir() {
        return getString("mq.data.dir", null);
    }

    public String[] getTopics() {
        String topics = getString("mq.topics", null);
        if (!StringUtils.isBlank(topics))
            return topics.split(",");
        return null;
    }

    public int getReplicaFetchSize() {
        return getInt("mq.replica.fetch.size", 30);
    }

    public int getReplicaFetchInterval() {
        return getInt("mq.replica.fetch.interval", 2);
    }

    public long getDataPersistenceInterval() {
        int value = getInt("mq.data.persistence.interval", 2);
        return (long) value;
    }

    public int getZKDataPersistenceInterval() {
        int value = getInt("mq.zk.data.persistence.interval", 6);
        return value;
    }

    public boolean getEnableZookeeper() {
        return getBoolean("mq.enable.zookeeper", false);
    }

    public String getBrokerGroupName() {
        return getString("mq.group.name", null);
    }

}
