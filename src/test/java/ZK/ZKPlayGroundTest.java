package ZK;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.junit.*;
import org.junit.runners.MethodSorters;

import java.util.concurrent.TimeUnit;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ZKPlayGroundTest {


    private String ZkConnectStr="127.0.0.1:2181";
    private String testPath="/RootTest001";

    private ZkClient zk;

    private String nodeName = "/myApp";

    private boolean isrun=true;

    @Before
    public void initTest() {

        zk = new ZkClient("127.0.0.1:2181");
    }

    @After
    public void dispose() {
        zk.close();

    }

    @Test
    public void testListener001() throws InterruptedException {
        //监听指定节点的数据变化

        zk.createPersistent("/rootxx/childyy",true);
        Thread.sleep(1000L);
        zk.deleteRecursive("/rootxx/childyy");
        if (zk.exists("/rootxx/childyy")){
            System.out.println(12334);
        }

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


//    @Test
//    public void testListener002() throws InterruptedException {
//        if (!zk.exists(nodeName)) {
//            zk.createPersistent(nodeName);
//        }
//        zk.writeData(nodeName, "1");
//        zk.writeData(nodeName, "2");
//        zk.delete(nodeName);
//        zk.delete(nodeName);//删除一个不存在的node，并不会报错
//        this.isrun=false;
//
//    }

}
