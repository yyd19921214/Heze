package ZK;

import com.yudy.heze.util.ZkUtils;
import com.yudy.heze.zk.ZkClient;
import org.junit.*;
import org.junit.runners.MethodSorters;

import java.util.List;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ZKUtilTest {



    static ZkClient zkClient;
    static String zkpath="/HEZEMQTEST7/child";

    @BeforeClass
    public static void init(){
        zkClient=new ZkClient("127.0.0.1:2181","user:password");
    }

    @Test
    public void test001_makeSurePersistentPathExist(){
        String s=ZkUtils.makeSurePersistentPathExist(zkClient,zkpath);
        Assert.assertNotNull(s);
        Assert.assertTrue(zkClient.exists(zkpath));
        zkClient.delete(zkpath);
    }

    @Test
    public void test002_getChildrenParentMayNotExist(){
        ZkUtils.makeSurePersistentPathExist(zkClient,zkpath);
        List<String> list=ZkUtils.getChildrenParentMayNotExist(zkClient,zkpath.substring(0,zkpath.lastIndexOf("/")));
        Assert.assertTrue(list.size()>0);
        zkClient.delete(zkpath);

        List<String> list2=ZkUtils.getChildrenParentMayNotExist(zkClient,zkpath.replace("HEZEMQ","heze"));
        Assert.assertTrue(list2==null);
    }

    @Test
    public void test003_createEphemeralPath(){
        String s=ZkUtils.createEphemeralPath(zkClient,zkpath,"hello");
        Assert.assertTrue(s!=null);
    }

    @AfterClass
    public static void close(){
        zkClient.close();
    }


}
