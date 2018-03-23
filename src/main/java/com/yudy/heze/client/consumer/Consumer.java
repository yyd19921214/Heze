package com.yudy.heze.client.consumer;

import com.yudy.heze.config.ServerConfig;
import com.yudy.heze.network.Topic;

import java.util.HashSet;
import java.util.Set;

public class Consumer {

    private Set<String> topics=new HashSet<>();

    private long ZK_COUNTER_MAX;
    private long zkCount=0;


    private Consumer(){;}

    private static Consumer INSTANCE=new Consumer();

    public static Consumer getInstance() {
        return INSTANCE;
    }

    public void connect(ServerConfig config){
        if (config.getEnableZookeeper()){

        }
        if (config.getTopics()!=null){
            for (String topic:config.getTopics()){
                topics.add(topic);
            }
        }

        ZK_COUNTER_MAX=config.getDataPersistenceInterval()/2;
        zkCount=ZK_COUNTER_MAX;
    }

    public static void fetch(){;}


}
