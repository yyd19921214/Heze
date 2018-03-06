package com.yudy.heze.heze.Netty;

import com.yudy.heze.serializer.NettyDecoder;
import com.yudy.heze.serializer.NettyEncode;
import com.yudy.heze.server.NettyServerHandler;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

public class DemoServer {

    private EventLoopGroup bossGroup;

    private EventLoopGroup workerGroup;

    private ChannelFuture f;

    private int port;

    public DemoServer(int port) {
        this.port = port;

    }

    public void startup() throws InterruptedException {
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
                        new DemoServerHandler()

                );

            }
        });

        f = b.bind(port).sync();
        long m1 = System.currentTimeMillis();
        System.out.println(m1);
        f.channel().closeFuture().sync();
        System.out.println("----------------------");
        System.out.println(System.currentTimeMillis());
//        workerGroup.shutdownGracefully();
//        bossGroup.shutdownGracefully();

    }

    public void stop() {
        workerGroup.shutdownGracefully();
        bossGroup.shutdownGracefully();
        if (f != null)
            f.channel().close();


    }


    public static void main(String[] args) {
        DemoServer s1 = new DemoServer(808);

        Thread t0 = new Thread(
                () -> {
                    try {
                        s1.startup();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
        );
        t0.start();


        Thread t1 = new Thread(
                () -> {
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    s1.stop();

                }
        );
        t1.start();

    }


}
