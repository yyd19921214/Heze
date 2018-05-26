package com.yudy.heze.client.producer;

import com.google.common.cache.*;
import com.google.common.collect.Lists;
import com.yudy.heze.client.NettyClient;
import com.yudy.heze.config.ServerConfig;
import com.yudy.heze.network.Message;
import com.yudy.heze.network.Topic;
import com.yudy.heze.network.TransferType;
import com.yudy.heze.server.RequestHandler;
import com.yudy.heze.util.DataUtils;
import com.yudy.heze.util.ZkUtils;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;


public class BasicProducer {

    //todo add scheduler to notify producer when new server register in zookeeper---done
    //todo add aysnc producer mechanism---done not very perfect
    //todo use pool of netty client---done
    //todo add ack mechanism/fail retry mechanism

    private static final BasicProducer INSTANCE = new BasicProducer();

    private final static Logger LOGGER = LoggerFactory.getLogger(BasicProducer.class);

    private ZkClient zkClient;

    private LoadingCache<String, NettyClient> nettyClientCache;

    public Map<String, String> serverIpMap = new ConcurrentHashMap<>();

    private Random rand = new Random();


    private BasicProducer() {

    }

    public static BasicProducer getInstance() {
        return INSTANCE;
    }

    public void init(String path) {
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
                subscribeRefresh(zk_path);
            }
            zkClient.subscribeChildChanges(ZkUtils.ZK_BROKER_GROUP, new IZkChildListener() {
                @Override
                public void handleChildChange(String s, List<String> list) throws Exception {
                    if (!CollectionUtils.isEmpty(list)) {
                        list.stream().filter(child -> !serverIpMap.containsKey(child) && zkClient.exists(ZkUtils.ZK_BROKER_GROUP + "/" + child)).forEach(
                                child -> {
                                    String zk_path = ZkUtils.ZK_BROKER_GROUP + "/" + child;
                                    String ipPort = zkClient.readData(zk_path);
                                    serverIpMap.put(child, ipPort);
                                    subscribeRefresh(zk_path);
                                }
                        );
                    }
                }

            });

        }

        nettyClientCache = CacheBuilder.newBuilder().removalListener(
                (RemovalNotification<String, NettyClient> notify)->notify.getValue().stop()
        ).expireAfterAccess(10, TimeUnit.SECONDS).maximumSize(100)
                .build(new CacheLoader<String, NettyClient>() {
                    @Override
                    public NettyClient load(String s) throws Exception {
                        int count = 0;
                        String ip=s.split(":")[0];
                        int port=Integer.parseInt(s.split(":")[1]);
                        while (count < 2) {
                            NettyClient client = new NettyClient();
                            try {
                                client.open(ip, port);
                            } catch (Exception e) {
                                e.printStackTrace();
                                client.stop();
                                client = new NettyClient();
                            }
                            if (client.isConnected()){
                                return client;
                            }
                            count++;
                        }
                        return null;
                    }
                });
    }


    public boolean send(Topic topic) {
        return send(topic, new RandomPartitioner());
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
            return send(topic, destAddress,params);
        }
        return false;
    }

    private boolean send(Topic topic, String destAddress,Map<String, String> params) {
        return send(Lists.newArrayList(topic), destAddress,params);
    }

    public boolean send(Topic[] topics, Map<String, String> params) {
        return send(Arrays.asList(topics), params);
    }


    public boolean send(List<Topic> topics, Map<String, String> params) {
        String destAddress = null;
        if (params.containsKey("broker")) {
            String brokerName = params.get("broker");
            destAddress = serverIpMap.get(brokerName);
        } else if (params.containsKey("brokerAddr")) {
            destAddress = params.get("brokerAddr");
        }
        if (StringUtils.isNotBlank(destAddress)) {
            return send(topics, destAddress,params);
        }
        return false;
    }

    private boolean send(List<Topic> topics, String destAddress,Map<String, String> params) {
        boolean result = false;
        NettyClient client=getNettyClientFromCache(destAddress);
        if (client!=null) {
            Message request = Message.newRequestMessage();
            request.setReqHandlerType(RequestHandler.PRODUCER);
            request.setBody(DataUtils.serialize(topics));
            if (Boolean.valueOf(params.getOrDefault("async","false"))){
                try {
                    int retryCnt=0;
                    while (!result&&retryCnt<3){
                        Message response = client.write(request);
                        if (response == null || response.getType() == TransferType.EXCEPTION.value) {
                            result = false;
                            retryCnt++;
                        } else {
                            result = true;
                        }
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            else{
                result=client.writeAsync(request);
            }
        }
        return result;
    }

    private void subscribeRefresh(String zk_path) {
        zkClient.subscribeDataChanges(zk_path, new IZkDataListener() {
            @Override
            public void handleDataChange(String s, Object o) throws Exception {
                System.out.println(s + " data is changed");
                String changedServer = s.split("/")[3];
                if (serverIpMap.containsKey(changedServer)) {
                    serverIpMap.put(changedServer, (String) o);
                }
            }

            @Override
            public void handleDataDeleted(String s) throws Exception {
                System.out.println(s + " is deleted...");
                String removeServer = s.split("/")[3];
                if (serverIpMap.containsKey(removeServer)) {
                    serverIpMap.remove(removeServer);
                }
            }
        });
    }


    private NettyClient getNettyClientFromCache(String path){
        try {
            NettyClient client=nettyClientCache.get(path);
            return client;
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        return null;
    }


    public void stop() {
        nettyClientCache.invalidateAll();
        nettyClientCache=null;
        zkClient.unsubscribeAll();
        zkClient = null;
    }


    class RandomPartitioner implements Function<Topic, String> {
        @Override
        public String apply(Topic topic) {
            List<String> brokers = serverIpMap.keySet().stream().collect(Collectors.toList());
            String brokerName = brokers.get(rand.nextInt(brokers.size()));
            return brokerName;
        }
    }


}
