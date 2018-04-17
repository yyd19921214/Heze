package Store;

import com.yudy.heze.config.ServerConfig;
import com.yudy.heze.store.BasicTopicQueuePool;
import org.I0Itec.zkclient.ZkClient;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.File;
import java.util.Arrays;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class BasicPoolTest {

    private String queueName="PoolQueue";


    static ZkClient zkClient;

    static String ZkConnectStr="127.0.0.1:2181";

    @BeforeClass
    public static void zkInit(){
        zkClient=new ZkClient(ZkConnectStr,4000);
    }

    @Test
    public void test001_create(){
        ZkClient zkClient = new ZkClient("127.0.0.1:2181", 4000);
        ServerConfig config=new ServerConfig("conf/config.properties");
        BasicTopicQueuePool.startup(zkClient,config);
        Assert.assertTrue(BasicTopicQueuePool.getQueue(queueName)==null);
        Assert.assertTrue(BasicTopicQueuePool.getQueueOrCreate(queueName)!=null);
//        zkClient.countChildren(BasicTopicQueuePool.)

        BasicTopicQueuePool.destory();
        doClean();
    }


    private void doClean() {
        File f = new File(BasicTopicQueuePool.DEFAULT_DATA_PATH);
        Arrays.stream(f.listFiles(File::isFile)).forEach(File::delete);
    }

}
