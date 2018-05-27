package com.yudy.heze.network;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Topic implements Serializable {

    private static final long serialVersionUID = 1L;

    private String topic;

    private long readOffset;

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

    public void setContent(Serializable content) {
        this.content = content;
    }

    public long getReadOffset() {
        return readOffset;
    }

    public void setReadOffset(long readOffset) {
        this.readOffset = readOffset;
    }


    public String toString() {
        return String.format("topic:%s,offset:%d,content:%s", topic, readOffset, content.toString());
    }

}
