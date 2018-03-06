package com.yudy.heze.serializer;

import com.yudy.heze.network.Message;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class NettyEncode extends MessageToByteEncoder<Message>{

    @Override
    protected void encode(ChannelHandlerContext channelHandlerContext, Message message, ByteBuf byteBuf) throws Exception {
        message.writeToByteBuf(byteBuf);

    }

    public static void main(String[] args) {
        System.out.println(Integer.MAX_VALUE);
    }

}
