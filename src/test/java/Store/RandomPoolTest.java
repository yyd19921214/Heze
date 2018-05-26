package Store;

import com.yudy.heze.config.ServerConfig;
import com.yudy.heze.store.pool.RandomAccessQueuePool;
import org.I0Itec.zkclient.ZkClient;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.File;
import java.util.Arrays;

import static com.yudy.heze.util.ZkUtils.ZK_BROKER_GROUP;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class RandomPoolTest {

    private String queueName="topic-hello";

    @Test
    public void test001_create(){
        ZkClient zkClient = new ZkClient("127.0.0.1:2181", 4000);
        ServerConfig config=new ServerConfig("conf/config.properties");
        zkClient.createPersistent(ZK_BROKER_GROUP+"/"+config.getServerName(),true);
        RandomAccessQueuePool.startup(zkClient,config);
        Assert.assertTrue(RandomAccessQueuePool.getQueue(queueName)==null);
        Assert.assertTrue(RandomAccessQueuePool.getQueueOrCreate(queueName)!=null);
        RandomAccessQueuePool.destroy();
        zkClient.deleteRecursive(ZK_BROKER_GROUP);
        doClean();
    }


    private void doClean() {
        File f = new File(RandomAccessQueuePool.DEFAULT_DATA_PATH);
        Arrays.stream(f.listFiles(File::isFile)).forEach(File::delete);
    }


}
