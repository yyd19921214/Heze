package Server;

import com.yudy.heze.config.Config;
import com.yudy.heze.server.BasicServer;
import com.yudy.heze.util.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.junit.*;
import org.junit.runners.MethodSorters;

import java.io.IOException;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class BasicServerTest {

    private BasicServer basicServer;


    static ZkClient zkClient;

    static String ZkConnectStr="127.0.0.1:2181";

    @BeforeClass
    public static void zkInit(){
        zkClient=new ZkClient(ZkConnectStr,4000);
    }

    @Test
    public void test001_Start(){
        basicServer=new BasicServer();
        basicServer.startup("conf/config.properties");
        Assert.assertNotNull(basicServer);
        Assert.assertTrue(zkClient.exists(basicServer.getZkPath()));
        Assert.assertNotNull(zkClient.readData(basicServer.getZkPath()));
    }

    @AfterClass
    public static void doClean(){
        zkClient.deleteRecursive(ZkUtils.ZK_BROKER_GROUP);
        zkClient.close();
    }

}
