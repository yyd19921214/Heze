package com.yudy.heze.server.handlers;

import com.yudy.heze.network.Message;
import com.yudy.heze.network.Topic;
import com.yudy.heze.server.RequestHandler;
import com.yudy.heze.store.pool.BasicTopicQueuePool;
import com.yudy.heze.store.pool.RandomAccessQueuePool;
import com.yudy.heze.util.DataUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ProducerRequestHandler implements RequestHandler {

    private final static Logger LOGGER = LoggerFactory.getLogger(ProducerRequestHandler.class);

    @Override
    public Message handler(Message request) {
        if (null!=request.getBody()){
            List<Topic> topics= (List<Topic>) DataUtils.deserialize(request.getBody());

            if (topics!=null){
                for (Topic topic:topics){
                    RandomAccessQueuePool.getQueueOrCreate(topic.getTopic()).append(DataUtils.serialize(topic.getContent()));
                }
                LOGGER.info("Producer request handler, receive message:"+topics.toString());
            }
            else{
                LOGGER.info("Producer request handler, receive message is null.");
            }
        }
        Message response=Message.newResponseMessage();
        response.setSeqId(request.getSeqId());
        return response;
    }
}
