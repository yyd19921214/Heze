package com.yudy.heze.client.consumer;

import com.yudy.heze.client.NettyClient;
import com.yudy.heze.config.ServerConfig;
import com.yudy.heze.exception.SendRequestException;
import com.yudy.heze.exception.TimeoutException;
import com.yudy.heze.network.Message;
import com.yudy.heze.network.Topic;
import com.yudy.heze.network.TransferType;
import com.yudy.heze.server.RequestHandler;
import com.yudy.heze.util.DataUtils;
import com.yudy.heze.util.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static com.yudy.heze.util.ZkUtils.ZK_BROKER_GROUP;

public class SimpleConsumer implements IConsumer {

    private final static Logger LOGGER = LoggerFactory.getLogger(SimpleConsumer.class);


    private Map<String, Long> subscribeTopics;
    private ZkClient zkClient;
    private NettyClient nettyClient;
    private Map<String, Set<String>> serverTopicMap;  //server1-><topic1,topic2,topic3....>
    private Map<String, Set<String>> topicServerMap;  //topic1-><server1,server2,server3....>
    private Map<String, String> serverInfoMap; // ip address and port for each server

    private int batchSize;

    private String assignServerAddress;


    public SimpleConsumer(ServerConfig config) {
        subscribeTopics = new ConcurrentHashMap<>();
        nettyClient = new NettyClient();
        nettyClient.initZKClient(config);
        zkClient = nettyClient.zkClient;
        serverTopicMap = new HashMap<>();
        serverInfoMap = new HashMap<>();
        topicServerMap = new HashMap<>();
        initTopicInfo();
        assignServerAddress = ChoiceServer();
        //batchSize = config.getInt("max.batch.records", 1);
        //assignServerAddress = connectionStr;
    }


    @Override
    public List<Topic> poll() {
        if (reConnect()) {
            return fetch(subscribeTopics.keySet().toArray(new String[0]));
        }
        return null;
    }

    @Override
    public boolean subscribe(List<String> topics) {
        if (topics.stream().anyMatch(t -> !topicServerMap.containsKey(t))) {
            throw new IllegalArgumentException("unExisted topics in any broker");
        }
        topics.forEach(t -> subscribeTopics.put(t, 0L));
        return false;
    }

    @Override
    public void close() throws IOException {

    }

    private List<Topic> fetch(String[] topics) {
        return fetch(Arrays.asList(topics));
    }

    private List<Topic> fetch(List<String> topics) {
        List<Topic> topicList = new ArrayList<>();
        List<Topic> rtTopics = null;
        topics.forEach(t -> {
            Topic topic = new Topic();
            topic.setTopic(t);
            topic.setReadOffset(subscribeTopics.get(t));
            topicList.add(topic);
        });
        Message request = Message.newRequestMessage();
        request.setReqHandlerType(RequestHandler.FETCH);
        request.setBody(DataUtils.serialize(topicList));
        try {
            Message response = nettyClient.write(request);
            if (response.getType() == TransferType.EXCEPTION.value) {
                // 有异常
                System.out.println("Cuonsumer fetch message error");
                LOGGER.error("Cuonsumer fetch message error");
            } else {
                if (null != response.getBody()) {
                    rtTopics = (List<Topic>) DataUtils.deserialize(response.getBody());
                }
            }
        } catch (TimeoutException e) {
            throw e;
        } catch (SendRequestException e) {
            throw e;
        }
        return rtTopics;
    }

    // the structure of zk is like:
    // /ZKRootPath/ServerName/{Topic1,Topic2....}
    // server ip and port info is stored in /ZKRootPath/ServerName node
    private boolean initTopicInfo() {
        if (!zkClient.exists(ZK_BROKER_GROUP)) {
            throw new IllegalArgumentException("No any brokers existed");
        }
        List<String> servers = zkClient.getChildren(ZK_BROKER_GROUP);
        for (String server : servers) {
            String serverInfo = zkClient.readData(ZK_BROKER_GROUP + "/" + server);
            List<String> topicsList = zkClient.getChildren(ZK_BROKER_GROUP + "/" + server);
            if (CollectionUtils.isNotEmpty(topicsList)) {
                if (!serverTopicMap.containsKey(server)) {
                    Set<String> topicPerServer = new HashSet<>();
                    serverTopicMap.put(server, topicPerServer);
                }
                serverTopicMap.get(server).addAll(topicsList);
                for (String topic : topicsList) {
                    if (!topicServerMap.containsKey(topic)) {
                        Set<String> serverPerTopic = new HashSet<>();
                        topicServerMap.put(topic, serverPerTopic);
                    }
                    topicServerMap.get(topic).add(server);
                }
            }
            serverInfoMap.put(server, serverInfo);
        }
        // todo add logic of monitor zk (broker add and remove....)
        return true;
    }

    private boolean reConnect() {
        if (!nettyClient.isConnected()) {
            String host = assignServerAddress.split(":")[0];
            int port = Integer.parseInt(assignServerAddress.split(":")[1]);
            try {
                nettyClient.open(host, port);
            } catch (IllegalStateException e) {
                nettyClient.setConnected(false);
                LOGGER.error(String.format("consumer %s:%d error：", host, port));
            } catch (Exception e) {
                nettyClient.setConnected(false);
                LOGGER.error(String.format("consumer %s:%d error：", host, port));
            }
        }
        return nettyClient.isConnected();
    }

    private String ChoiceServer() {
        return serverInfoMap.values().toArray(new String[0])[0];
    }
}
