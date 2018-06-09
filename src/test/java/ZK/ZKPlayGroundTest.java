package ZK;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.SerializableSerializer;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.junit.After;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ZKPlayGroundTest {


    private String ZkConnectStr="40.71.225.3:2181";

    private ZkClient zkClient;

    private String nodeName = "/myApp";

    private boolean isrun=true;

//    @Before
//    public void initTest() {
//
//        zkClient = new ZkClient(ZkConnectStr);
//    }
//
//    @After
//    public void dispose() {
//        zkClient.close();
//
//    }

    @Test
    public void test001_create(){
        zkClient = new ZkClient(ZkConnectStr,10000,10000,new SerializableSerializer());
        System.out.println("zk is connected");
//        zkClient.createPersistent("/ZkAlive");

    }

    @Test
    public void testListener001() throws InterruptedException {
        //监听指定节点的数据变化

//        zk.createPersistent("/rootxx/childyy",true);
//        Thread.sleep(1000L);
//        zk.deleteRecursive("/rootxx/childyy");
//        if (zk.exists("/rootxx/childyy")){
//            System.out.println(12334);
//        }

//        if (!zk.exists(nodeName)) {
//            zk.createPersistent(nodeName);
//        }
//        zk.subscribeDataChanges(nodeName, new IZkDataListener() {
//            @Override
//            public void handleDataChange(String s, Object o) throws Exception {
//                System.out.println("node data changed!");
//                System.out.println("node=>" + s);
//                System.out.println("data=>" + o);
//                System.out.println("--------------");
//            }
//
//            @Override
//            public void handleDataDeleted(String s) throws Exception {
//                System.out.println("node data deleted!");
//                System.out.println("s=>" + s);
//                System.out.println("--------------");
//
//
//            }
//        });

//        zk.subscribeDataChanges();



//        zk.readData(nodeName);
//        zk.writeData(nodeName, "1");
//        zk.writeData(nodeName, "2");
//        zk.delete(nodeName);
//        zk.delete(nodeName);//删除一个不存在的node，并不会报错


    }




}
