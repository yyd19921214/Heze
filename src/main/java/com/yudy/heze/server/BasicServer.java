package com.yudy.heze.server;

import com.yudy.heze.config.ServerConfig;
import com.yudy.heze.serializer.NettyDecoder;
import com.yudy.heze.serializer.NettyEncode;
import com.yudy.heze.store.TopicQueuePool;
import com.yudy.heze.util.PortUtils;
import com.yudy.heze.util.ZkUtils;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.collection.IntObjectHashMap;
import io.netty.util.collection.IntObjectMap;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;


public class BasicServer implements MServer{

    private static final Logger LOGGER = LoggerFactory.getLogger(NettyServer.class);

    private EventLoopGroup bossGroup;

    private EventLoopGroup workerGroup;

    private ChannelFuture f;

    private final IntObjectMap<RequestHandler> handlerMap = new IntObjectHashMap<>(128);




    @Override
    public boolean startup(String configName) {
        File configFile;
        try {
            configFile = new File(configName).getCanonicalFile();
        } catch (IOException e) {
            LOGGER.error("fail to read config from file");
            e.printStackTrace();
            return false;
        }
        if (!configFile.exists() || !configFile.isFile()) {
            LOGGER.error(String.format("ERROR: Main config file not exist => '%s', copy one from 'conf/server.properties.sample' first.", configFile.getAbsolutePath()));
            return false;
        }
        final ServerConfig config = new ServerConfig(configFile);
        start(config);
        return true;
    }

    private void start(ServerConfig config) {
        LOGGER.info("server is starting...");
        PortUtils.checkAvailablePort(config.getPort());
        ServerBootstrap b = configServer();
        try {
            if (StringUtils.isNotBlank(config.getHost())) {
                f = b.bind(config.getHost(), config.getPort()).sync();
            } else {
                f = b.bind(config.getPort()).sync();
            }
            Runtime.getRuntime().addShutdownHook(new BasicServer.ShutdownThread());
        } catch (InterruptedException e) {
//            LOGGER.error("Exception happen when start server", e);
            e.printStackTrace();
        }



        //todo register in zk
        //todo init existed queue poll
        //todo init embeded producecr

        if (config.getEnableZookeeper()){
            ZkClient zkClient = new ZkClient(config.getZkConnect(), config.getZkConnectionTimeoutMs());
            zkClient.createPersistentSequential(ZkUtils.ZK_BROKER_GROUP,config.getHost()+":"+config.getPort());
        }

//        TopicQueuePool.startup(zkClient,config);
//
//        if (config.getReplicaHost()!=null){
//            EmbeddedConsumer.getInstance().start(config);
//        }

    }

    private ServerBootstrap configServer() {
        bossGroup = new NioEventLoopGroup();
        workerGroup = new NioEventLoopGroup();

        ServerBootstrap b = new ServerBootstrap();

        b.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 1024).option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_TIMEOUT, 6000)
                .childOption(ChannelOption.SO_REUSEADDR, true).childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);

        b.childHandler(new ChannelInitializer() {
            @Override
            protected void initChannel(Channel channel) throws Exception {
                channel.pipeline().addLast(
                        new NettyDecoder(),
                        new NettyEncode(),
                        new NettyServerHandler(handlerMap)

                );

            }
        });
        return b;
    }

    class ShutdownThread extends Thread {
        @Override
        public void run() {
            try {
                BasicServer.this.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void close() throws IOException {
//        LOGGER.info("Netty server is stoping");
        System.out.println("do some clean work before shutdown");
        if(f.channel()!=null)
            f.channel().close();
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
        //todo unregister from zk
        //todo close embededProducer
        //todo delete all queue file in disk;

//        EmbeddedConsumer.getInstance().stop();
//        LOGGER.info("Netty server stopped");
        System.out.println("Netty server stopped");

    }

    private boolean recovery(){
        //todo recover form failup
        //todo copy data from slave
        return true;
    }
}
