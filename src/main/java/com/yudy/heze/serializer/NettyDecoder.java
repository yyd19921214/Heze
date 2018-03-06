package com.yudy.heze.serializer;

import com.yudy.heze.network.Message;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

public class NettyDecoder extends LengthFieldBasedFrameDecoder{

    public NettyDecoder(){
        super(Integer.MAX_VALUE, 0, Message.FRAME_LEN, 0, Message.FRAME_LEN);
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        ByteBuf byteBuf= (ByteBuf) super.decode(ctx,in);
        if (byteBuf==null)
            return null;
        Message msg=new Message();
        msg.readFromByteBuf(byteBuf);
        return msg;
    }



}
