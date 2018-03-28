package com.yudy.heze.server.backup;

import com.yudy.heze.client.NettyClient;
import com.yudy.heze.config.ServerConfig;
import com.yudy.heze.exception.SendRequestException;
import com.yudy.heze.network.Backup;
import com.yudy.heze.network.Message;
import com.yudy.heze.network.Topic;
import com.yudy.heze.network.TransferType;
import com.yudy.heze.server.RequestHandler;
import com.yudy.heze.util.DataUtils;
import com.yudy.heze.util.Scheduler;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class EmbeddedConsumer {
    private static final Logger LOGGER= LoggerFactory.getLogger(EmbeddedConsumer.class);

    private static final EmbeddedConsumer INSTANCE = new EmbeddedConsumer();

    private final Scheduler scheduler=new Scheduler(1,"heze-mq-embeded-consumer",false);

    private String replicaHost;
    private int port;
    private NettyClient nettyClient;
    private Set<String> topics=new HashSet<>();

    private EmbeddedConsumer(){}

    public EmbeddedConsumer getInstance(){
        return INSTANCE;
    }

    public void start(ServerConfig config) {
        replicaHost=config.getReplicaHost();
        port=config.getPort();
        if (StringUtils.isNotBlank(replicaHost)){
            nettyClient=new NettyClient();
            //todo add scheduler
//            this.scheduler.scheduleWithRate()
        }
    }

    public void stop(){
        this.nettyClient.stop();
        this.scheduler.shutdown();
    }

    class ReplicaConsumerRunnable implements Runnable{

        int errorCnt=0;
        @Override
        public void run() {
            if (errorCnt>2){
                if (INSTANCE.nettyClient!=null){
                    INSTANCE.nettyClient.stop();
                }
                INSTANCE.nettyClient=new NettyClient();
                errorCnt=0;
            }
            if (!INSTANCE.nettyClient.isConnected()){
                try{
                    INSTANCE.nettyClient.open(INSTANCE.replicaHost,INSTANCE.port);
                }catch (IllegalStateException ex){
                    errorCnt++;
                    ex.printStackTrace();
                    LOGGER.error("Embedded consumer connection error.", ex);
                }catch (Exception ex){
                    ex.printStackTrace();
                    LOGGER.error("Embedded consumer connection error.", ex);
                }
            }else{
                Message request = Message.newRequestMessage();
                request.setReqHandlerType(RequestHandler.REPLICA);
                Set<String> queues=BackupQueuePool.getBackupQueueNameFromDisk();
                if (!CollectionUtils.isEmpty(queues)){
                    List<Backup> backups=new ArrayList<>();
                    for (String queue:queues){
                        BackupQueue backupQueue=BackupQueuePool.getBackupQueueFromPool(queue);
                        int wNum=backupQueue.getWriteIndex().getWriteNum();
                        int wPos=backupQueue.getWriteIndex().getWritePosition();
                        int wCounter=backupQueue.getWriteIndex().getWriteCounter();
                        backups.add(new Backup(queue,wNum,wPos,wCounter));
                    }
                    request.setBody(DataUtils.serialize(backups));
                }
                try{
                    Message response=INSTANCE.nettyClient.write(request);
                    if (response.getType()!= TransferType.EXCEPTION.value){
                        if (response.getBody()!=null){
                            List<Topic> backupResult= (List<Topic>) DataUtils.deserialize(response.getBody());
                            if (!CollectionUtils.isEmpty(backupResult)){
                                for (Topic topic:backupResult){
                                    BackupQueue backupQueue=BackupQueuePool.getBackupQueueFromPool(topic.getTopic());
                                    backupQueue.offer(DataUtils.serialize(topic));
                                }

                            }

                        }
                    }
                    if (response.getBody().length>0){
                        LOGGER.info("fetch data from master broker ,size:"+response.bodyLength());
                    }

                }catch (SendRequestException e) {
                    INSTANCE.nettyClient.setConnected(false);
                    LOGGER.error("Embedded consumer flush topic error.", e);
                }catch (Exception e) {
                    INSTANCE.nettyClient.setConnected(false);
                    errorCnt++;
                    LOGGER.error("Embedded consumer flush topic error.", e);
                }

            }

        }
    }



}
