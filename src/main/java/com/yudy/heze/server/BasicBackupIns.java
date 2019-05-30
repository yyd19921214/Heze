package com.yudy.heze.server;

import com.yudy.heze.client.NettyClient;
import com.yudy.heze.network.Message;
import com.yudy.heze.network.Topic;
import com.yudy.heze.network.TransferType;
import com.yudy.heze.util.DataUtils;
import com.yudy.heze.util.ZkUtils;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * the only thing this class will do is pull data from master continuely
 */
public class BasicBackupIns {

    private volatile boolean inSlaveMode;

    private long downloadInterval;

    private String masterServer;

    private String masterUrl;

    private int port;

    private final ScheduledExecutorService scheduler;

    private ZkClient zkClient;

    private Map<String,Long> backupTopics=new ConcurrentHashMap<>();

    private NettyClient nettyClient;


    public BasicBackupIns(boolean inSlaveMode, long downloadInterval, String masterServer, ZkClient zkClient) {
        this.inSlaveMode = inSlaveMode;
        this.downloadInterval = downloadInterval;
        this.masterServer = masterServer;
        this.zkClient = zkClient;

        List<String> topics=this.zkClient.getChildren(ZkUtils.ZK_BROKER_GROUP+"/"+masterServer);
        for (String topic:topics){
            backupTopics.put(topic,0L);
        }

        String urlPort = zkClient.readData(ZkUtils.ZK_BROKER_GROUP + "/" + masterServer);
        this.masterUrl= urlPort.split(":")[0];
        this.port= Integer.parseInt(urlPort.split(":")[1]);

        nettyClient = new NettyClient();

        this.zkClient.subscribeChildChanges(ZkUtils.ZK_BROKER_GROUP + "/" + masterServer, new IZkChildListener() {
            @Override
            public void handleChildChange(String s, List<String> list) throws Exception {
                if (!CollectionUtils.isEmpty(list)) {
                    list.stream().filter(t->!backupTopics.containsKey(t)).forEach(t->backupTopics.put(t,0L));
                }
            }
        });

        scheduler = Executors.newScheduledThreadPool(1);

    }

    public void stopSlaveMode() {
        inSlaveMode = false;
    }

    public void backup() {
        final ScheduledFuture<?> backupHandle = scheduler.scheduleAtFixedRate(
                () -> {
                    nettyClient.open(this.masterUrl,this.port);
                    if (!nettyClient.isConnected()){
                        System.out.println("can not connect to master server normally");
                    }
                    List<Topic> requestTopicList = new ArrayList<>();
                    Message request = Message.newRequestMessage();
                    request.setReqHandlerType(RequestHandler.REPLICA);

                    for (String topicName:backupTopics.keySet()){
                        Topic topic=new Topic();
                        topic.setTopic(topicName);
                        topic.setReadOffset(backupTopics.get(topicName));
                        requestTopicList.add(topic);
                    }
                    request.setBody(DataUtils.serialize(requestTopicList));
                    Message response=nettyClient.write(request);

                    if (response.getType() == TransferType.EXCEPTION.value) {
                        // 有异常
                        System.out.println("backup fetch message error");
                    } else {
                        if (null != response.getBody() && response.getBody().length > 0) {
                            List<Topic> rtTopics = (List<Topic>) DataUtils.deserialize(response.getBody());
                            //TODO we should use more efficient way to download data
                            //SaveLocal(rtTopics);
                        }
                    }
                }, 1, downloadInterval, SECONDS);
    }


}
