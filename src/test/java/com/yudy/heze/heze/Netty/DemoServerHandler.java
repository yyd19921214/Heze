package com.yudy.heze.heze.Netty;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public class DemoServerHandler extends SimpleChannelInboundHandler<String> {

    @Override
    protected void messageReceived(ChannelHandlerContext channelHandlerContext, String o) throws Exception {
        channelHandlerContext.writeAndFlush("get request"+o);
    }
}
