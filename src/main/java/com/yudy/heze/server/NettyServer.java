package com.yudy.heze.server;

import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class NettyServer {
    private static final Logger LOGGER= LoggerFactory.getLogger(NettyServer.class);

    private EventLoopGroup bossGroup;

    private EventLoopGroup workerGroup;

    private ChannelFuture f;

//    private ServerRegi

    public void start(int port){
        Properties properties=new Properties();
        properties.setProperty("port",String.valueOf(port));
        start(properties);
    }

    public void start(Properties properties){
//        start(new Ser);
    }





}
