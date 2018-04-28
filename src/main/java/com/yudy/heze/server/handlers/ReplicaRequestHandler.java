package com.yudy.heze.server.handlers;

import com.yudy.heze.config.ServerConfig;
import com.yudy.heze.network.Backup;
import com.yudy.heze.network.Message;
import com.yudy.heze.network.Topic;
import com.yudy.heze.server.AbstractRequestHandler;
import com.yudy.heze.store.queue.TopicQueue;
import com.yudy.heze.store.pool.TopicQueuePool;
import com.yudy.heze.util.DataUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;


public class ReplicaRequestHandler extends AbstractRequestHandler {

    public ReplicaRequestHandler(ServerConfig config) {
        super(config);
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(ReplicaRequestHandler.class);

    @Override
    public Message handler(Message request) {
        int fetchSize = config.getReplicaFetchSize();
        List<Topic> result=new ArrayList<>();
        final List<Backup> backups = (List<Backup>) DataUtils.deserialize(request.getBody());
        Set<String> queueSet = TopicQueuePool.getQueueNameFromDisk();
        if (!CollectionUtils.isEmpty(queueSet) && !CollectionUtils.isEmpty(backups)) {
            queueSet.removeAll(backups.stream().filter(b -> queueSet.contains(b.getQueueName())).
                    map(Backup::getQueueName).collect(Collectors.toSet()));
            queueSet.forEach(queueName ->
                    backups.add(new Backup(queueName, 0, 0, 0))
            );
        }
        if (!CollectionUtils.isEmpty(backups)) {
            for (Backup backup : backups) {
                TopicQueue queue = TopicQueuePool.getQueue(backup.getQueueName());
                if (null != queue) {
                    byte[] tpc;
                    int slaveReadNum = backup.getSlaveWriteNum();
                    int slaveReadPos = backup.getSlaveWritePosition();
                    for (int i = 0; i < fetchSize; i++) {
                        tpc = queue.replicaRead(slaveReadNum, slaveReadPos);
                        if (null != tpc) {
                            slaveReadPos+=tpc.length+4;
                            Topic tmp= (Topic) DataUtils.deserialize(tpc);
                            result.add(tmp);
                        }
                        else{
                            break;
                        }
                    }

                }
            }
        }
        Message resp=Message.newResponseMessage();
        resp.setSeqId(request.getSeqId());
        if (result.size()>0){
            resp.setBody(DataUtils.serialize(result));
        }
        return resp;
    }

    public static void main(String[] args) {
        final Set<String> st1 = new HashSet<>();
        st1.add("hello");
        System.out.println(st1.size());

    }
}
