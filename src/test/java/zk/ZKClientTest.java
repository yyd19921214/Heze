package zk;

import com.yudy.heze.zk.ZkClient;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.List;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ZKClientTest {

    static ZkClient zkClient=null;

    private String nodePath="/HEZEMQTEST3/child";

    @BeforeClass
    public static void init(){
        zkClient=new ZkClient("127.0.0.1:2181","user:password");
    }

    @Test
    public void test001_connect(){
        Assert.assertTrue(zkClient.isConnected());
    }

    @Test
    public void test002_createPersistent(){
        String s=zkClient.createPersistent(nodePath,true);
        Assert.assertTrue(zkClient.exists(s));
        zkClient.delete(s);
    }

    @Test
    public void test003_createEphemeral(){
        String s=zkClient.createEphemeral(nodePath,"hello".getBytes());
        Assert.assertTrue(zkClient.exists(s));
        zkClient.delete(s);
    }


    @Test
    public void test004_createEphemeralSequential(){
        String s1=zkClient.createEphemeralSequential(nodePath,"hello".getBytes());
        String s2=zkClient.createEphemeralSequential(nodePath,"hello".getBytes());
        Assert.assertTrue(zkClient.exists(s1));
        Assert.assertTrue(zkClient.exists(s2));
        Assert.assertTrue(Integer.parseInt(s1.replace(nodePath,""))-Integer.parseInt(s2.replace(nodePath,""))==-1);
        zkClient.delete(s1);
        zkClient.delete(s2);
    }

    @Test
    public void test005_GetChildren(){
        zkClient.createPersistent(nodePath,true);
        List<String> childList=zkClient.getChildren(nodePath.substring(0,nodePath.lastIndexOf("/")));
        Assert.assertNotNull(childList);
        zkClient.delete(nodePath);
    }


    @Test
    public void test006_subscribeChildChanges(){
        zkClient.createPersistent("/HEZEMQTEST3",true);
        List<String> r=zkClient.subscribeChildChanges("/HEZEMQTEST3",(parent,currentChildren)->{
            System.out.println(111);
            System.out.println(currentChildren.get(0));
        });
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        zkClient.createPersistent(nodePath,true);

    }



    @Test
    public void test099_delete(){
        zkClient.createPersistent(nodePath,true);
        zkClient.delete(nodePath);
        Assert.assertFalse(zkClient.exists(nodePath));
    }


    @Test
    public void test100_close(){
        zkClient.close();
        Assert.assertFalse(zkClient.isConnected());
    }


}
