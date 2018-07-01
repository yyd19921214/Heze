package com.yudy.heze.server.handlers;

import com.yudy.heze.network.Message;
import com.yudy.heze.network.Topic;
import com.yudy.heze.server.RequestHandler;
import com.yudy.heze.store.pool.RandomAccessQueuePool;
import com.yudy.heze.store.queue.RandomAccessTopicQueue;
import com.yudy.heze.util.DataUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;


public class ReplicaRequestHandler implements RequestHandler {

    private final static Logger LOGGER = LoggerFactory.getLogger(ReplicaRequestHandler.class);

    @Override
    public Message handler(Message request) {
        System.out.println("get fetch request!!!");
        List<Topic> results = new ArrayList<>();
        List<Topic> topics = (List<Topic>) DataUtils.deserialize(request.getBody());
        if (topics != null) {
            for (Topic topic : topics) {
                RandomAccessTopicQueue queue = RandomAccessQueuePool.getQueue(topic.getTopic());
                long maxNow=queue.getMaxOffset();
                if(maxNow>topic.getReadOffset()){
                    for (long offset=topic.getReadOffset()+1;offset<maxNow;offset++){
                        byte[] rtn=queue.read(offset);
                        String content=(String) DataUtils.deserialize(rtn);
                        Topic tmp = new Topic();
                        tmp.setContent(content);
                        tmp.setReadOffset(offset);
                        tmp.setTopic(topic.getTopic());
                        results.add(tmp);
                    }
                }
            }
        }
        Message response=Message.newResponseMessage();
        response.setSeqId(request.getSeqId());
        if (results.size()>0){
            response.setBody(DataUtils.serialize(results));
            LOGGER.info("replica request handler, message:"+results.toString());
        }
        return response;
    }
}
