package com.yudy.heze.server;

import com.yudy.heze.config.ServerConfig;
import com.yudy.heze.serializer.NettyDecoder;
import com.yudy.heze.serializer.NettyEncode;
import com.yudy.heze.server.backup.EmbeddedConsumer;
import com.yudy.heze.store.TopicQueuePool;
import com.yudy.heze.util.PortUtils;
import com.yudy.heze.zk.ZkClient;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.collection.IntObjectHashMap;
import io.netty.util.collection.IntObjectMap;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class NettyServer {
    private static final Logger LOGGER = LoggerFactory.getLogger(NettyServer.class);

    private EventLoopGroup bossGroup;

    private EventLoopGroup workerGroup;

    private ChannelFuture f;

    private final IntObjectMap<RequestHandler> handlerMap = new IntObjectHashMap<>(128);

//    private ServerRegi

    public void start(int port) {
        Properties properties = new Properties();
        properties.setProperty("port", String.valueOf(port));
        start(properties);
    }

    public void start(String configFileName) {
        File configFile = null;
        try {
            configFile = new File(configFileName).getCanonicalFile();
        } catch (IOException e) {
            LOGGER.error("fail to read config from file");
            e.printStackTrace();
        }
        if (!configFile.exists() || !configFile.isFile()) {
            LOGGER.error(String.format("ERROR: Main config file not exist => '%s', copy one from 'conf/server.properties.sample' first.", configFile.getAbsolutePath()));
            System.exit(2);
        }
        final ServerConfig config = new ServerConfig(configFile);
        start(config);

    }

    public void start(Properties properties) {
        start(new ServerConfig(properties));
    }

    public void start(ServerConfig config) {
        LOGGER.info("server is starting...");
        int port = PortUtils.checkAvailablePort(config.getPort());
        ServerBootstrap b = configServer();
        try {
            if (StringUtils.isNotBlank(config.getHost())) {
                f = b.bind(config.getHost(), config.getPort()).sync();
            } else {
                f = b.bind(config.getPort()).sync();
            }
            Runtime.getRuntime().addShutdownHook(new ShutdownThread());
        } catch (InterruptedException e) {
            LOGGER.error("Exception happen when start server", e);
            e.printStackTrace();
        }

        ZkClient zkClient=null;
        if (config.getEnableZookeeper()){
            ServerRegister serverRegister=new ServerRegister();
            zkClient=serverRegister.startup(config);
        }

        TopicQueuePool.startup(zkClient,config);

        if (config.getReplicaHost()!=null)
            EmbeddedConsumer.getInstance().start(config);

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


    public void stop(){
        LOGGER.info("Netty server is stoping");
        if(f.channel()!=null)
            f.channel().close();
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
        //TODO embeddedConsumer.getInstance().stop
        LOGGER.info("Netty server stopped");
        System.out.println("Netty server stopped");
    }

    public void waitForClose() throws InterruptedException {
        f.channel().closeFuture().sync();

    }


    public void registerHandler(int handlerId,RequestHandler requestHandler){
        handlerMap.put(handlerId,requestHandler);
    }


    class ShutdownThread extends Thread {
        @Override
        public void run() {
            NettyServer.this.stop();
        }
    }




    public static void main(String[] args) {
        String configFileName = "D:\\xxl-mq\\xxl-mq\\xxl-mq-broker\\pom.xml";
        try {
            File configFile = new File(configFileName).getCanonicalFile();
            if (configFile.isFile())
                System.out.println("yes");
            BufferedReader b = new BufferedReader(new FileReader(configFile));
            String readLine = "";
            while ((readLine = b.readLine()) != null)
                System.out.println(readLine);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


}
