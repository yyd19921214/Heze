package com.yudy.heze.heze;

import com.yudy.heze.client.producer.Producer;
import com.yudy.heze.network.Topic;

import java.util.ArrayList;
import java.util.List;

public class ProducerTest {

    public static void main(String[] args) {
        String cfg = "file:D:\\heze\\conf\\config.properties";
        Producer.getInstance().connect(cfg);
//        for(int i=0;i<10000;i++){
//            List<Topic> list = new ArrayList<Topic>();
        Topic topic = new Topic();
        topic.setTopic("umq");
        topic.addContent("umq作者juny=>");
//            list.add(topic);
        Producer.getInstance().send(topic);
//        }
    }


}
