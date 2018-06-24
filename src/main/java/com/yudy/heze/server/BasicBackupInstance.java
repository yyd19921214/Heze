package com.yudy.heze.server;

import com.yudy.heze.network.Message;
import com.yudy.heze.network.Topic;
import com.yudy.heze.util.ZkUtils;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.collections4.CollectionUtils;

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
public class BasicBackupInstance {

    private volatile boolean inSlaveMode;

    private long downloadInterval;

    private String masterServer;

    private final ScheduledExecutorService scheduler;

    private ZkClient zkClient;

    private Map<String,Long> backupTopics=new ConcurrentHashMap<>();


    public BasicBackupInstance(boolean inSlaveMode, long downloadInterval, String masterServer, ZkClient zkClient) {
        this.inSlaveMode = inSlaveMode;
        this.downloadInterval = downloadInterval;
        this.masterServer = masterServer;
        this.zkClient = zkClient;

        List<String> topics=this.zkClient.getChildren(ZkUtils.ZK_BROKER_GROUP+"/"+masterServer);
        for (String topic:topics){
            backupTopics.put(topic,0L);
        }

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
                    String urlPort = zkClient.readData(ZkUtils.ZK_BROKER_GROUP + "/" + masterServer);
                    String url = urlPort.split(":")[0];
                    int port = Integer.parseInt(urlPort.split(":")[1]);

                    Message request = Message.newRequestMessage();
                    request.setReqHandlerType(RequestHandler.REPLICA);
                    for (String topic:backupTopics.keySet()){

                    }
                    request.setBody(null);



                }, 1, downloadInterval, SECONDS);
    }


}
