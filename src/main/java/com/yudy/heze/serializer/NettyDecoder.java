package com.yudy.heze.serializer;

import com.yudy.heze.server.NettyServer;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

public class NettyDecoder extends LengthFieldBasedFrameDecoder{

    public NettyDecoder(){
        super(Integer.MAX_VALUE, 0, NettyEncoder.FRAME_LEN, 0, NettyEncoder.FRAME_LEN);
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        return super.decode(ctx, in);
    }
}
