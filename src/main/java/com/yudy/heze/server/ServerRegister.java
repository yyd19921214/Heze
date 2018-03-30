package com.yudy.heze.server;

import com.yudy.heze.config.ServerConfig;
import com.yudy.heze.zk.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.yudy.heze.util.ZkUtils.ZK_MQ_BASE;

public class ServerRegister {

    private static final Logger LOGGER= LoggerFactory.getLogger(ServerRegister.class);

    private ServerConfig config;
    private ZkClient zkClient;


    public static final String ZK_BROKER_GROUP = ZK_MQ_BASE + "/brokergroup";

    public ZkClient startup(ServerConfig config) {
        LOGGER.info("connecting to zookeeper: " + config.getZkConnect());
        this.config=config;
        if (zkClient==null){
            String authString=config.getZkUsername()+":"+config.getZkPassword();
            zkClient=new ZkClient()
        }




        //todo

        return null;

    }
}
