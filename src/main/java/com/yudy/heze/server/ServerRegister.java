package com.yudy.heze.server;

import com.yudy.heze.config.ServerConfig;
import com.yudy.heze.zk.ZkClient;

import static com.yudy.heze.util.ZkUtils.ZK_MQ_BASE;

public class ServerRegister {

    public static final String ZK_BROKER_GROUP = ZK_MQ_BASE + "/brokergroup";

    public ZkClient startup(ServerConfig config) {
        return null;

    }
}
