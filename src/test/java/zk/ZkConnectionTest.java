package zk;

import com.yudy.heze.zk.ZkConnection;
import org.apache.zookeeper.*;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ZkConnectionTest {



    static String zkServer="127.0.0.1:2181";
    static int sessionTimeOut=Integer.MAX_VALUE;
    static String authStr="user:password";
    static ZkConnection zkConnection;

    private String zkTreePath="/ZkTreeRoot4";
    private String childPath=zkTreePath+"/child";



    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        zkConnection=new ZkConnection(zkServer,sessionTimeOut,authStr);
    }

    @Test
    public void test001_Connect(){
        CountDownLatch latch=new CountDownLatch(1);
        Watcher w=new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                if (watchedEvent.getState()== Event.KeeperState.SyncConnected){
                    latch.countDown();
                }
            }
        };
        zkConnection.connect(w);
        try {
            latch.await(10000L, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        Assert.assertTrue(latch.getCount()==0);
    }

    @Test
    public void test002_Create() throws UnsupportedEncodingException, KeeperException, InterruptedException {
        String s=zkConnection.create(zkTreePath,"hello".getBytes("utf-8"), CreateMode.PERSISTENT);
        Assert.assertTrue(zkConnection.getZookeeper().exists(zkTreePath,false)!=null);
    }

    @Test
    public void test003_exist() throws KeeperException, InterruptedException {
        Assert.assertTrue(zkConnection.exists(zkTreePath,false));
    }

    @Test
    public void test004_getChildren(){
        try {
            zkConnection.create(childPath,null, CreateMode.PERSISTENT);
        } catch (KeeperException|InterruptedException e) {
            e.printStackTrace();
        }
        List<String> childs=null;
        try {
            childs=zkConnection.getChildren("/",false);
        } catch (KeeperException|InterruptedException e) {
            e.printStackTrace();
        }
        Assert.assertNotNull(childs);
        System.out.println(childs);
        try {
            zkConnection.delete(childPath);
        } catch (KeeperException|InterruptedException e) {
            e.printStackTrace();
        }
    }


    @Test
    public void test005_readData(){
        byte[] readData=null;
        try {
            readData=zkConnection.readData(zkTreePath,null,false);
        } catch (KeeperException|InterruptedException e) {
            e.printStackTrace();
        }
        Assert.assertTrue(new String(readData).equals("hello"));
    }



    @Test
    public void test006_writeData(){
        try {
            zkConnection.writeData(zkTreePath,"world".getBytes(),-1);
        } catch (KeeperException|InterruptedException e) {
            e.printStackTrace();
        }
        byte[] readData=null;
        try {
            readData=zkConnection.readData(zkTreePath,null,false);
        } catch (KeeperException|InterruptedException e) {
            e.printStackTrace();
        }
        Assert.assertTrue(new String(readData).equals("world"));
    }

    @Test
    public void test007_getZKState(){
        Assert.assertTrue(zkConnection.getZookeeperState()== ZooKeeper.States.CONNECTED);
    }




    @Test
    public void test099_Delete() throws KeeperException, InterruptedException {
        try {
            zkConnection.delete(zkTreePath);
        } catch (KeeperException|InterruptedException e) {
            e.printStackTrace();
        }
        Assert.assertTrue(zkConnection.getZookeeper().exists(zkTreePath,false)==null);
    }





    @Test
    public void test100_Close(){
        try {
            zkConnection.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        Assert.assertTrue(zkConnection.getZookeeperState()==null);
    }











}
