package com.yudy.heze.client.producer;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.yudy.heze.client.NettyClient;
import com.yudy.heze.config.ServerConfig;
import com.yudy.heze.network.Message;
import com.yudy.heze.network.Topic;
import com.yudy.heze.network.TransferType;
import com.yudy.heze.server.RequestHandler;
import com.yudy.heze.util.DataUtils;
import com.yudy.heze.util.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;


public class BasicProducer {

    //todo add scheduler to notify producer when new server register in zookeeper
    //todo add fail retry mechanism
    //todo use clientPool instead of singleton
    //todo add ack mechanism

    private static final BasicProducer INSTANCE = new BasicProducer();

    private final static Logger LOGGER = LoggerFactory.getLogger(BasicProducer.class);

    public NettyClient client = null;

    private ZkClient zkClient;

    public Map<String, String> serverIpMap = new ConcurrentHashMap<>();

    private Random rand = new Random();

    public volatile String currentAddress;

    private BasicProducer() {

    }

    public static BasicProducer getInstance() {
        return INSTANCE;
    }

    public void init(String path) {
        client = new NettyClient();
        File mainFile = null;
        try {
            URL url = new URL(path);
            mainFile = new File(url.getFile()).getCanonicalFile();
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(2);
        }
        if (!mainFile.isFile() || !mainFile.exists()) {
            System.out.println(path + " is not existed or is not a file");
            LOGGER.error(String.format("ERROR: Main config file not exist => '%s', copy one from 'conf/server.properties.sample' first.", mainFile.getAbsolutePath()));
            System.exit(2);

        }
        ServerConfig config = new ServerConfig(mainFile);
        zkClient = new ZkClient(config.getZkConnect(), config.getZkConnectionTimeoutMs());
        if (zkClient.exists(ZkUtils.ZK_BROKER_GROUP)) {
            List<String> children = zkClient.getChildren(ZkUtils.ZK_BROKER_GROUP);

            for (String child : children) {
                String zk_path = ZkUtils.ZK_BROKER_GROUP + "/" + child;
                String ipPort = zkClient.readData(zk_path);

                serverIpMap.put(child, ipPort);
            }
        }
    }


    public boolean send(Topic topic) {
        return send(topic,new RandomPartitioner());
    }

    public boolean send(Topic topic, Function<Topic, String> f) {
        Map<String, String> params = new HashMap<>();
        params.put("broker", f.apply(topic));
        return send(topic, params);
    }


    public boolean send(Topic topic, Map<String, String> params) {

        String destAddress = null;
        if (params.containsKey("broker")) {
            String brokerName = params.get("broker");
            destAddress = serverIpMap.get(brokerName);
        } else if (params.containsKey("brokerAddr")) {
            destAddress = params.get("brokerAddr");
        }
        if (StringUtils.isNotBlank(destAddress)) {
            String ip = destAddress.split(":")[0];
            int port = Integer.parseInt(destAddress.split(":")[1]);
            return send(topic, ip, port);
        }
        return false;
    }

    private boolean send(Topic topic,String destIp, int destPort){

        return send(Lists.newArrayList(topic),destIp,destPort);
    }

    public boolean send(Topic[] topics,Map<String, String> params){
        return send(Arrays.asList(topics), params);
    }


    public boolean send(List<Topic> topics,Map<String, String> params){
        String destAddress = null;
        if (params.containsKey("broker")) {
            String brokerName = params.get("broker");
            destAddress = serverIpMap.get(brokerName);
        } else if (params.containsKey("brokerAddr")) {
            destAddress = params.get("brokerAddr");
        }
        if (StringUtils.isNotBlank(destAddress)) {
            String ip = destAddress.split(":")[0];
            int port = Integer.parseInt(destAddress.split(":")[1]);
            return send(topics, ip, port);
        }
        return false;
    }



    private boolean send(List<Topic> topics, String destIp, int destPort) {
        boolean result = false;
        if (reconnect(destIp, destPort)) {
            Message request = Message.newRequestMessage();
            request.setReqHandlerType(RequestHandler.PRODUCER);
            request.setBody(DataUtils.serialize(topics));

            try {
                Message response = client.write(request);
                if (response == null || response.getType() == TransferType.EXCEPTION.value) {
                    result = false;
                } else {
                    result = true;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

        }

        return result;

    }


    public boolean reconnect(String ip, int port) {
        int count = 0;
        while ((currentAddress == null || !currentAddress.equals(ip) || !client.isConnected()) && (count < 2)) {
            try {
                client.open(ip, port);
            } catch (Exception e) {
                e.printStackTrace();
                client.stop();
                client = new NettyClient();
                count++;
            }

            if (client.isConnected()) {
                currentAddress = ip;
            }
        }
        return client.isConnected();

    }

    public void stop() {
        client.stop();
    }


    class RandomPartitioner implements Function<Topic,String>{
        @Override
        public String apply(Topic topic) {
            List<String> brokers = serverIpMap.keySet().stream().collect(Collectors.toList());
            String brokerName = brokers.get(rand.nextInt(brokers.size()));
            return brokerName;
        }
    }


}
