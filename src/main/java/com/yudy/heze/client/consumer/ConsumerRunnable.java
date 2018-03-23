package com.yudy.heze.client.consumer;

import com.yudy.heze.config.ServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerRunnable extends Thread {


    private final static Logger LOG= LoggerFactory.getLogger(ConsumerRunnable.class);

    private volatile boolean stop=false;

    public ConsumerRunnable(ServerConfig config){
        super("Heze-mq-consumer");
        Consumer.getInstance().connect(config);
    }



    @Override
    public void run() {
        try{
            while (!stop){
                Consumer.fetch();
                Thread.sleep(500);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void start(){
        super.start();
    }

    public void close(){
        stop=true;
        this.interrupt();
    }
}
