package com.yudy.heze.server;

import com.yudy.heze.cluster.Cluster;
import com.yudy.heze.cluster.Group;
import com.yudy.heze.config.ServerConfig;
import com.yudy.heze.util.DataUtils;
import com.yudy.heze.util.ZkUtils;
import com.yudy.heze.zk.ZkClient;
import org.apache.zookeeper.ZKUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;

import static com.yudy.heze.util.ZkUtils.ZK_MQ_BASE;

public class ServerRegister implements Closeable{

    private static final Logger LOGGER = LoggerFactory.getLogger(ServerRegister.class);

    private ServerConfig config;
    private ZkClient zkClient;


    public static final String ZK_BROKER_GROUP = ZK_MQ_BASE + "/brokergroup";

    public ZkClient startup(ServerConfig config) {
        LOGGER.info("connecting to zookeeper: " + config.getZkConnect());
        this.config = config;
        if (zkClient == null) {
            String authString = config.getZkUsername() + ":" + config.getZkPassword();
            zkClient = new ZkClient(config.getZkConnect(), authString, config.getZkSessionTimeoutMs(), config.getZkConnectionTimeoutMs());
        }
        registerBrokerGroupInZk();
        ZkUtils.getCluster(zkClient);
        return zkClient;
    }

    private void registerBrokerGroupInZk() {
        String zkPath = ZK_BROKER_GROUP;
        ZkUtils.makeSurePersistentPathExist(zkClient, zkPath);        
        LOGGER.info("registering broker group" + zkPath);
        Group brokerGroup = new Group(config.getBrokerGroupName(), config.getHost(), config.getPort());
        zkPath += "/" + brokerGroup.getName();
        String jsonGroup = DataUtils.brokerGroup2Json(brokerGroup);
        ZkUtils.getCluster(zkClient);
        if (!Cluster.getMasterIps().contains(config.getHost())) {
            ZkUtils.createEphemeralPathExpectConflict(zkClient, zkPath, jsonGroup);
            Cluster.setCurrent(brokerGroup);
        }
    }

    @Override
    public void close() throws IOException {
        if (zkClient!=null){
            LOGGER.info("closing zkclient now....");
            zkClient.close();
        }
    }
}
