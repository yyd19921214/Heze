package zk;

import org.I0Itec.zkclient.ZkClient;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ZKPlayGroundTest {

    private ZkClient zkClient;
    private String ZkConnectStr="127.0.0.1:2181";
    private String testPath="/RootTest001";

    @Test
    public void test001(){
        zkClient=new ZkClient(ZkConnectStr,4000);

        zkClient.createPersistent(testPath+"/child1",true);

        zkClient.createPersistent(testPath+"/child2",true);

        Assert.assertTrue(zkClient.getChildren(testPath).size()==2);
        System.out.println(zkClient.exists(testPath));
        zkClient.deleteRecursive(testPath);

        System.out.println(zkClient.exists(testPath));

//        ;

//        zkClient.createPersistent(testPath);
//        String s=zkClient.createPersistentSequential(testPath+"/child",null);
//
//        System.out.println(s);
//        zkClient.createPersistent(testPath);
//        Assert.assertTrue(zkClient.exists(testPath));
//        zkClient.delete(s);
//        Assert.assertFalse(zkClient.exists(testPath));
        zkClient.close();

    }


}
