package com.yudy.heze.heze;

import com.yudy.heze.client.producer.Producer;
import com.yudy.heze.network.Topic;

public class ProducerTest {

    public static void main(String[] args) {
        String cfg = "file:D:\\heze\\conf\\config.properties";
        Producer.getInstance().connect(cfg);
        Topic topic = new Topic();
        topic.setTopic("umq");
        topic.addContent("hellotony");
        Producer.getInstance().send(topic);
        System.out.println("finished write....");

    }


}
