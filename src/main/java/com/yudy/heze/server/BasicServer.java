package com.yudy.heze.server;

import com.yudy.heze.client.NettyClient;
import com.yudy.heze.config.ServerConfig;
import com.yudy.heze.network.Message;
import com.yudy.heze.network.Topic;
import com.yudy.heze.network.TransferType;
import com.yudy.heze.serializer.NettyDecoder;
import com.yudy.heze.serializer.NettyEncode;
import com.yudy.heze.store.pool.BasicTopicQueuePool;
import com.yudy.heze.store.pool.RandomAccessQueuePool;
import com.yudy.heze.util.DataUtils;
import com.yudy.heze.util.PortUtils;
import com.yudy.heze.util.Scheduler;
import com.yudy.heze.util.ZkUtils;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.collection.IntObjectHashMap;
import io.netty.util.collection.IntObjectMap;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;


public class BasicServer implements MServer{

    //todo init an embeded producer to make replica
    //todo close embeded producer when BasicServer close;
    //todo add recovery mechanism
    //todo copy data from slave

    private static final Logger LOGGER = LoggerFactory.getLogger(NettyServer.class);

    private EventLoopGroup bossGroup;

    private EventLoopGroup workerGroup;

    private ChannelFuture f;

    private final IntObjectMap<RequestHandler> handlerMap = new IntObjectHashMap<>(128);

    private ZkClient zkClient;

    private String zkPath;

    private Thread shutDownHook;

    private volatile boolean inSlaveMode;

    private NettyClient nettyClient;

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
        if (config.getServerName()==null){
            throw new IllegalArgumentException("Must set a Name for this broker");
        }
        //register in zk
        zkClient = new ZkClient(config.getZkConnect(), config.getZkConnectionTimeoutMs());
        zkPath=ZkUtils.ZK_BROKER_GROUP+"/"+config.getServerName();
        if (zkClient.exists(zkPath)){
            throw new IllegalArgumentException("A same name broker has alrady existed...");
        }
        zkClient.createPersistent(zkPath,true);
        zkClient.writeData(zkPath,config.getHost()+":"+config.getPort());

        RandomAccessQueuePool.startup(zkClient,config);

        if (config.getIsSlaveOf()!=null){
            System.out.println("server is run in slave mode");
            String masterPath=ZkUtils.ZK_BROKER_GROUP + "/" + config.getIsSlaveOf();
            if (!zkClient.exists(masterPath)){
                System.out.println("master server is not existed!");
                throw new IllegalArgumentException("master server is not existed!");
            }
            inSlaveMode=true;
            zkClient.subscribeDataChanges(masterPath, new IZkDataListener() {
                @Override
                public void handleDataChange(String s, Object o) throws Exception {

                }
                @Override
                public void handleDataDeleted(String s) throws Exception {
                    wakeUpFromSlave();
                }
            });

            String urlPort = zkClient.readData(masterPath);
            String masterUrl= urlPort.split(":")[0];
            int masterPort= Integer.parseInt(urlPort.split(":")[1]);
            nettyClient=new NettyClient();
            nettyClient.open(masterUrl,masterPort);

            //TODO start thread to pull data from master
            while (inSlaveMode){
                Set<String> topicNames=RandomAccessQueuePool.getAllQueueNames();

                List<Topic> requestTopicList=new ArrayList<>();
                for (String topicName:topicNames){
                    long maxOffset=RandomAccessQueuePool.getQueue(topicName).getMaxOffset();
                    Topic requestTopic=new Topic();
                    requestTopic.setTopic(topicName);
                    requestTopic.setReadOffset(maxOffset+1);
                    requestTopicList.add(requestTopic);
                }

                Message request=Message.newRequestMessage();
                request.setReqHandlerType(RequestHandler.REPLICA);
                request.setBody(DataUtils.serialize(requestTopicList));
                Message response=nettyClient.write(request);
                if (response.getType() != TransferType.EXCEPTION.value && response.getBody() != null && response.getBody().length > 0) {
                    List<Topic> rtTopics = (List<Topic>) DataUtils.deserialize(response.getBody());
                    for (Topic t : rtTopics) {
                        String rtName = t.getTopic();
                        RandomAccessQueuePool.getQueueOrCreate(rtName).append(DataUtils.serialize(t.getContent()));
                    }
                }
                try {
                    Thread.sleep(1000*60*5L);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }


            }

        }

        ServerBootstrap b = configServer();
        try {
            if (StringUtils.isNotBlank(config.getHost())) {
                f = b.bind(config.getHost(), config.getPort()).sync();
            } else {
                f = b.bind(config.getPort()).sync();
            }
            shutDownHook=new BasicServer.ShutdownThread();
            Runtime.getRuntime().addShutdownHook(shutDownHook);

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
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
        if(f.channel()!=null)
            f.channel().close();
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
        RandomAccessQueuePool.destroy();
        if (zkClient!=null&&StringUtils.isNotBlank(zkPath)&&zkClient.exists(zkPath)){
            zkClient.deleteRecursive(zkPath);
            zkClient.close();
        }
        if(nettyClient!=null){
            nettyClient.stop();
        }
        LOGGER.info("Netty server stopped");
        System.out.println("Netty server stopped");

    }

    public void directClose() throws IOException {
        Runtime.getRuntime().removeShutdownHook(shutDownHook);
        close();
    }

    public void waitForClose() throws InterruptedException {
        f.channel().closeFuture().sync();

    }

    public void registerHandler(int handlerId,RequestHandler requestHandler){
        handlerMap.put(handlerId,requestHandler);
    }

    public String getZkPath() {
        return zkPath;
    }

    public ZkClient getZkClient() {
        return zkClient;
    }

    private boolean recovery(){
        return true;
    }

    private void wakeUpFromSlave(){
        inSlaveMode=false;
        LOGGER.info("wakeup from slave now");
    }

}
