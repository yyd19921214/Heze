package com.yudy.heze.heze;

import com.yudy.heze.zk.ZkConnection;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;
import org.w3c.dom.events.EventException;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.apache.zookeeper.CreateMode.PERSISTENT;
import static org.apache.zookeeper.KeeperException.Code.NONODE;

class AbstractZooKeeper implements Watcher {


    //缓存时间
    private static final int SESSION_TIME = 2000;
    protected ZooKeeper zooKeeper;
    protected CountDownLatch countDownLatch = new CountDownLatch(1);

    public void connect(String hosts) throws IOException, InterruptedException {
        zooKeeper = new ZooKeeper(hosts, SESSION_TIME, this);
        countDownLatch.await();
    }

    @Override
    public void process(WatchedEvent event) {
        // TODO Auto-generated method stub
        if (event.getState() == Event.KeeperState.SyncConnected) {
            countDownLatch.countDown();
        }
    }

    public void close() throws InterruptedException {
        zooKeeper.close();
    }
}

public class ZKTest {


    public static void main(String[] args) throws IOException, InterruptedException, NoSuchAlgorithmException, KeeperException {
        CountDownLatch latch = new CountDownLatch(1);
        ZooKeeper zk = new ZooKeeper("127.0.0.1:2181", 10000, event -> {
            // 监控所有被触发的事件
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                latch.countDown(); // 倒数-1
                System.out.println("已经触发了" + event.getState() + "事件！");
            }

        });
        String authStr = "user:password";
        zk.addAuthInfo("digest", authStr.getBytes());
        List<ACL> acl = new ArrayList<>();
        acl.add(new ACL(ZooDefs.Perms.ALL, new Id("digest",
                DigestAuthenticationProvider.generateDigest(authStr))));
        acl.add(new ACL(ZooDefs.Perms.READ, ZooDefs.Ids.ANYONE_ID_UNSAFE));
        latch.await();
        System.out.println("链接成功");

//        zk.create("/HEZEMQ/brokergroup",null,acl,CreateMode.PERSISTENT);
//        System.out.println("创建成功");

//        if (zk.exists("/HEZEMQ",false)!=null){
//            System.out.println("/HEZEMQ存在");
//            zk.delete("/HEZEMQ",-1);
//        }
//        if (zk.exists("/HEZEMQ/brokergroup",false)!=null){
//            System.out.println("/HEZEMQ/brokergroup存在");
//        }

//        zk.ex

//        try{
//            zk.create("/HEZEMQ/brokergroup", null, acl, CreateMode.PERSISTENT);
//        }catch (KeeperException.NoNodeException e){
//            System.out.println(1111);
//        }
//        String path="/HEZEMQ/brokergroup";
        List<String> child=zk.getChildren("/HEZEMQ",false);

//        zk.delete("/HEZEMQ/brokergroup",-1);;
        zk.delete("/HEZEMQ",-1);
//        System.out.println(child);
//        zk.delete("/HEZEMQ",-1);

//        try{
//            zk.create(path,null,acl, PERSISTENT);
//        }catch (KeeperException e){
//            if (e.code()== NONODE){
//                String parent=path.substring(0,path.lastIndexOf("/"));
//                zk.create(parent,null,acl, PERSISTENT);
//            }
//            zk.create(path,null,acl, PERSISTENT);
//        }



//        if (zk.exists("/HEZEMQ/brokergroup",false)!=null){
//            zk.delete("/HEZEMQ/brokergroup",-1);
//            System.out.println("删除节点成功");
//        }
//        if (zk.exists("/HEZEMQ",false)!=null){
//            zk.delete("/HEZEMQ",-1);
//            System.out.println("删除节点成功2");
//        }
//        zk.create("/HEZEMQ/brokergroup", null, acl, CreateMode.PERSISTENT);
//        System.out.println("创建节点成功");

        zk.close();


    }

}
