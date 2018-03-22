package com.yudy.heze.server.handlers;

import com.yudy.heze.network.Message;
import com.yudy.heze.network.Topic;
import com.yudy.heze.server.RequestHandler;
import com.yudy.heze.store.TopicQueue;
import com.yudy.heze.store.TopicQueuePool;
import com.yudy.heze.util.DataUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class FetchRequestHandler implements RequestHandler {

    private final static Logger LOGGER = LoggerFactory.getLogger(FetchRequestHandler.class);

    @Override
    public Message handler(Message request) {
        List<Topic> results = new ArrayList<>();
        List<Topic> topics = (List<Topic>) DataUtils.deserialize(request.getBody());
        if (topics != null) {
            for (Topic topic : topics) {
                int readCounter = 0;
                byte[] tpc = null;
                boolean backupQueueOver = false;
                //todo
//                BackupQueue backupQueue = BackupQueuePool.getBackupQueueFromPool(topic.getTopic());
//                if(null != backupQueue){
//                    tpc = backupQueue.poll();
//                    readCounter = backupQueue.getReadIndex().getReadCounter();
//                    if(tpc == null && readCounter == backupQueue.getWriteIndex().getWriteCounter()){
//                        backupQueueOver = true;
//                    }
//                }
//                if(backupQueue == null || backupQueueOver){
                TopicQueue queue = TopicQueuePool.getQueue(topic.getTopic());
                if (null != queue) {
                    tpc = queue.poll();
                    readCounter = queue.getReadIndex().getReadCounter();
                }

                if (null != tpc) {
                    Topic tmp = (Topic) DataUtils.deserialize(tpc);
                    tmp.setReadCounter(readCounter);
                    results.add(tmp);
                }

            }
        }
        Message response=Message.newResponseMessage();
        response.setSeqId(request.getSeqId());
        if (results.size()>0){
            response.setBody(DataUtils.serialize(results));
            LOGGER.info("Fetch request handler, message:"+results.toString());
        }

        return response;
    }
}
