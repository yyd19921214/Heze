package com.yudy.heze.client;

import com.yudy.heze.config.ServerConfig;
import com.yudy.heze.exception.SendRequestException;
import com.yudy.heze.exception.TimeoutException;
import com.yudy.heze.network.Message;
import com.yudy.heze.serializer.NettyDecoder;
import com.yudy.heze.serializer.NettyEncode;
import com.yudy.heze.zk.ZkClient;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

public class NettyClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(NettyClient.class);

    private final Bootstrap bootstrap = new Bootstrap();

    private final EventLoopGroup eventLoopGroupWorker;

    private final static DefaultEventExecutorGroup defaultEventExecutorGroup = new DefaultEventExecutorGroup(4);

    private final ConcurrentMap<Integer, ResponseFuture> responseTable = new ConcurrentHashMap<>(256);

    private boolean connected;

    private Channel channel;

    public ZkClient zkClient;


    public NettyClient() {
        this.eventLoopGroupWorker = new NioEventLoopGroup();
    }

    public void initZKClient(ServerConfig config){
        if (config.getEnableZookeeper()&&zkClient==null){
            String authString=config.getZkUsername()+":"+config.getZkPassword();
            //todo enable session time and connection time
            this.zkClient=new ZkClient(config.getZkConnect(),authString);
        }

    }

    public void open(String host, int port) {
        if (!connected) {
            Bootstrap handler = this.bootstrap.group(eventLoopGroupWorker).channel(NioSocketChannel.class)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .option(ChannelOption.SO_KEEPALIVE, false)
                    .handler(new ChannelInitializer<SocketChannel>() {

                        @Override
                        protected void initChannel(SocketChannel socketChannel) throws Exception {
                            socketChannel.pipeline().addLast(
                                    defaultEventExecutorGroup,
                                    new NettyDecoder(),
                                    new NettyEncode(),
                                    new NettyClientHandler()

                            );
                        }
                    });

            ChannelFuture channelFuture = handler.connect(host, port);
            this.channel = channelFuture.channel();
            try {
                channelFuture.sync();
                LOGGER.info("connect {}:{} ok.", host, port);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            connected = true;
        }
    }

    public boolean isConnected() {
        return connected;
    }

    public void setConnected(boolean connected) {
        this.connected = connected;
    }

    public Message write(final Message request) throws TimeoutException,SendRequestException{
        final ResponseFuture responseFuture=new ResponseFuture(request.getSeqId());
        responseTable.put(responseFuture.getId(),responseFuture);
        Message response=null;
        if (channel!=null){
            this.channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture channelFuture) throws Exception {
                    if (channelFuture.isSuccess()){
                        responseFuture.setIsOk(true);
                    }
                    else{
                        responseFuture.setIsOk(false);
                        responseTable.remove(responseFuture.getId(),responseFuture);
                        responseFuture.setCause(channelFuture.cause());
                        responseFuture.setResponse(null);
                        LOGGER.warn("send a request to channel <{}> failed.\nREQ:{}", channelFuture.channel(), request);
                    }

                }
            });
        }
        try {
            response=responseFuture.waitResponse(5000, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (null==response){
            if (responseFuture.isOk()) {
                throw new TimeoutException(
                        String.format("wait response on the channel <%s> timeout 10 (s).", this.channel),
                        responseFuture.getCause()
                );
            } else {
                throw new SendRequestException(
                        String.format("send request to the channel <%s> failed.", this.channel),
                        responseFuture.getCause());
            }
        }
        return response;
    }

    public boolean writeAsync(final Message request){
        if (channel!=null) {
            this.channel.writeAndFlush(request);
            return true;
        }
        return false;
    }

    public void stop(){
        LOGGER.info("close channel:{}", this.channel);
        eventLoopGroupWorker.shutdownGracefully();
        defaultEventExecutorGroup.shutdownGracefully();
        if (this.channel!=null){
            this.channel.close();
        }
        connected=false;
        LOGGER.info("close channel:{} ok.", this.channel);

    }

    class NettyClientHandler extends SimpleChannelInboundHandler<Message> {

        @Override
        protected void messageReceived(ChannelHandlerContext channelHandlerContext, Message message) throws Exception {
            int id = message.getSeqId();
            ResponseFuture responseFuture = responseTable.get(id);
            if (responseFuture != null) {
                responseFuture.setResponse(message);
                responseFuture.release();
                responseTable.remove(id);
            } else
                LOGGER.warn("receive request id:{} response, but it's not in.", id);

        }
    }


}
