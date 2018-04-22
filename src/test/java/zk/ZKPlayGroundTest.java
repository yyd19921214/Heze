package zk;

import com.google.common.collect.Lists;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.collections4.CollectionUtils;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.Arrays;
import java.util.List;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ZKPlayGroundTest {

    private ZkClient zkClient;
    private String ZkConnectStr="127.0.0.1:2181";
    private String testPath="/RootTest001";

//    @Test
    public void test001(){
        zkClient=new ZkClient(ZkConnectStr,4000);

        zkClient.createPersistent(testPath+"/child1",true);

        zkClient.createPersistent(testPath+"/child2",true);

        Assert.assertTrue(zkClient.getChildren(testPath).size()==2);
        System.out.println(zkClient.exists(testPath));
        zkClient.deleteRecursive(testPath);

        System.out.println(zkClient.exists(testPath));
        zkClient.close();

    }

    @Test
    public void test002(){
        zkClient=new ZkClient(ZkConnectStr,4000);
        zkClient.createPersistent(testPath,true);
//        zkClient.subscribeChildChanges(testPath, new IZkChildListener() {
//            @Override
//            public void handleChildChange(String s, List<String> list) throws Exception {
//                System.out.println("123");
//                if (!CollectionUtils.isEmpty(list)){
//                    System.out.println(list.get(list.size()-1));
////                    System.out.println(list.get(0));
//                }
//
//            }
//        });
        zkClient.createPersistent(testPath+"/child1");

//        try {
//            Thread.sleep(2000L);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//
        zkClient.createPersistent(testPath+"/child1/child2");
        zkClient.watchForChilds("")
//        zkClient.subscribeStateChanges();
//        zkClient.getChildren(testPath).forEach(System.out::println);
//        try {
//            Thread.sleep(2000L);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//
//        zkClient.createPersistent(testPath+"/child3");
//        try {
//            Thread.sleep(2000L);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }

//        System.out.println(zkClient.exists(testPath));
        zkClient.deleteRecursive(testPath);
        System.out.println(zkClient.exists(testPath+"/child1/child2"));

//        System.out.println(zkClient.exists(testPath));
        zkClient.close();

    }


}
