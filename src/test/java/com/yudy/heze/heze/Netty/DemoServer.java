package com.yudy.heze.heze.Netty;

import com.yudy.heze.serializer.NettyDecoder;
import com.yudy.heze.serializer.NettyEncode;
import com.yudy.heze.server.NettyServerHandler;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class DemoServer {

    private EventLoopGroup bossGroup;

    private EventLoopGroup workerGroup;

    private ChannelFuture f;


    Lock l=new ReentrantLock();
    Condition c1=l.newCondition();

    Object o=new Object();

    volatile boolean isStart=false;

    private int port;

    public DemoServer(int port) {
        this.port = port;

    }

    public void startup() throws InterruptedException {
        l.lock();
        {

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
            isStart = true;
            c1.signalAll();
        }
        l.unlock();
        f.channel().closeFuture().sync();
        System.out.println("----------------------");
        System.out.println(System.currentTimeMillis());



    }

    public  void stop() throws InterruptedException {
        l.lock();
        {
            long m1 = System.currentTimeMillis();
            System.out.println(m1);
            while (!isStart){
                System.out.println("111111");
                c1.await();

            }
            if (f != null)
                f.channel().close();
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
            System.out.println("successfully stop");

        }
        l.unlock();



    }


    public static void main(String[] args) throws InterruptedException {
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



        Thread t1 = new Thread(
                () -> {

                    try {
                        s1.stop();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                }
        );
        t1.start();
        Thread.sleep(2000);
        t0.start();
//        Thread.sleep(2000);

    }


}
