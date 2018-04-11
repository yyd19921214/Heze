package com.yudy.heze.heze;

import com.yudy.heze.zk.ZkConnection;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
class AbstractZooKeeper implements Watcher {


    //缓存时间
    private static final int SESSION_TIME   = 2000;
    protected ZooKeeper zooKeeper;
    protected CountDownLatch countDownLatch=new CountDownLatch(1);

    public void connect(String hosts) throws IOException, InterruptedException{
        zooKeeper = new ZooKeeper(hosts,SESSION_TIME,this);
        countDownLatch.await();
    }

    @Override
    public void process(WatchedEvent event) {
        // TODO Auto-generated method stub
        if(event.getState()== Event.KeeperState.SyncConnected){
            countDownLatch.countDown();
        }
    }

    public void close() throws InterruptedException{
        zooKeeper.close();
    }
}

public class ZKTest {


    private static String connectionString="";
    public static void main(String[] args) throws IOException, InterruptedException {
        CountDownLatch latch=new CountDownLatch(1);
        ZooKeeper zk = new ZooKeeper("127.0.0.1:2181", 10000, new Watcher() {
            // 监控所有被触发的事件
            @Override
            public void process(WatchedEvent event) {

                if (event.getState() == Event.KeeperState.SyncConnected) {
                    latch.countDown(); // 倒数-1
                    System.out.println("已经触发了" + event.getState() + "事件！");
                }
            }
        });
        latch.await();

        System.out.println("链接成功");

        zk.close();


    }

}
