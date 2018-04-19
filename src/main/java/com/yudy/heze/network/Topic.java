package com.yudy.heze.network;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Topic implements Serializable {

    private static final long serialVersionUID = 1L;

    private String topic;

    private int readCounter;

    private Serializable content;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Serializable getContent() {
        return content;
    }

    public void setContent(Serializable content){
        this.content=content;
    }

    public int getReadCounter() {
        return readCounter;
    }

    public void setReadCounter(int readCounter) {
        this.readCounter = readCounter;
    }

    public String toString(){
        return String.format("topic:%s,counter:%d,content:%s", topic, readCounter, content.toString());
    }

}
