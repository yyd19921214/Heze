package com.yudy.heze.client.consumer;

import com.yudy.heze.network.Topic;

import java.io.Closeable;
import java.util.List;

public interface IConsumer extends Closeable{

    List<Topic> poll();

    boolean subscribe(List<String> topics);

}
