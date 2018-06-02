package com.yudy.heze.client.consumer;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.yudy.heze.client.NettyClient;
import com.yudy.heze.config.ServerConfig;
import com.yudy.heze.exception.SendRequestException;
import com.yudy.heze.exception.TimeoutException;
import com.yudy.heze.network.Message;
import com.yudy.heze.network.Topic;
import com.yudy.heze.network.TransferType;
import com.yudy.heze.server.RequestHandler;
import com.yudy.heze.util.DataUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MultiMap;
import org.apache.commons.collections4.map.MultiValueMap;
import com.google.common.collect.ArrayListMultimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import static com.yudy.heze.util.ZkUtils.ZK_BROKER_GROUP;

/**
 * most simple and basic consumer
 * each poll only fetch one record returned by server
 * only support sync fetch and process
 * no any cache used in this consumer
 * only one topic subscribe supported
 * very simple and fixed assigned algorithm used
 * no any commit logic
 */
public class BasicConsumer{

    private final static Logger LOGGER = LoggerFactory.getLogger(BasicConsumer.class);


    private ZkClient zkClient;
    private NettyClient nettyClient;
    private ListMultimap<String,String> topicServerMap;
    private Map<String, String> serverInfoMap; // ip&port for each server

    private final String subscribeTopic;
    private final String assignedServerInfo;

    private AtomicLong topicOffset;

    public BasicConsumer(ServerConfig config, String topic) {
        subscribeTopic = topic;
        topicOffset=new AtomicLong(1L);
        nettyClient = new NettyClient();
        nettyClient.initZKClient(config);
        zkClient = nettyClient.zkClient;
        topicServerMap= ArrayListMultimap.create();
        serverInfoMap=new HashMap<>();
        initTopicInfo();
        assignedServerInfo=getAssignedServer(subscribeTopic);
        }


    public List<Topic> poll() {
        if (reConnect()) {
            return fetch();
        }
        return null;
    }

    private List<Topic> fetch() {
        Topic topic=new Topic();
        topic.setTopic(subscribeTopic);
        topic.setReadOffset(topicOffset.get());

        List<Topic> topicList = new ArrayList<>();
        topicList.add(topic);

        List<Topic> rtTopics = null;

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
        topicOffset.getAndIncrement();
        return rtTopics;
    }

    public void close() {
        nettyClient.stop();
        zkClient.close();

    }




    // the structure of zk is like:
    // /ZKRootPath/ServerName/{Topic1,Topic2....}
    // server ip&port info is stored in /ZKRootPath/ServerName node
    private boolean initTopicInfo() {
        if (!zkClient.exists(ZK_BROKER_GROUP)) {
            throw new IllegalArgumentException("No any brokers existed");
        }
        List<String> servers = zkClient.getChildren(ZK_BROKER_GROUP);
        for (String server : servers) {
            String serverInfo = zkClient.readData(ZK_BROKER_GROUP + "/" + server);
            List<String> topicsList = zkClient.getChildren(ZK_BROKER_GROUP + "/" + server);
            if (CollectionUtils.isNotEmpty(topicsList)) {
                topicsList.forEach(t->topicServerMap.put(t,server));
            }
            serverInfoMap.put(server, serverInfo);
        }
        // todo add logic of monitor zk (broker add and remove....)
        return true;
    }

    private boolean reConnect() {
        if (!nettyClient.isConnected()) {
            String ip=assignedServerInfo.split(":")[0];
            int port = Integer.parseInt(assignedServerInfo.split(":")[1]);
            try {
                nettyClient.open(ip, port);
            } catch (IllegalStateException e) {
                nettyClient.setConnected(false);
                LOGGER.error(String.format("consumer %s:%d error：", ip, port));
            } catch (Exception e) {
                nettyClient.setConnected(false);
                LOGGER.error(String.format("consumer %s:%d error：", ip, port));
            }
        }
        return nettyClient.isConnected();
    }

    private String getAssignedServer(String topic){
        String serverName=topicServerMap.get(topic).get(0);
        String serverInfo=serverInfoMap.get(serverName);
        return serverInfo;
    }

}
