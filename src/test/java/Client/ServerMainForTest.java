package Client;

import com.yudy.heze.server.BasicServer;
import com.yudy.heze.server.RequestHandler;
import com.yudy.heze.server.handlers.FetchRequestHandler;
import com.yudy.heze.server.handlers.ProducerRequestHandler;
import com.yudy.heze.store.block.BasicTopicQueueBlock;
import com.yudy.heze.store.block.RandomAccessBlock;
import com.yudy.heze.store.queue.BasicTopicQueue;
import com.yudy.heze.util.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.junit.BeforeClass;


import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.yudy.heze.util.ZkUtils.ZK_BROKER_GROUP;

public class ServerMainForTest {


    //private static String configPath="conf/config.properties";
    private static ZkClient zkClient;
    //private static String ZkConnectStr = "40.71.225.3:2181";


    public static void main(String[] args) {
        zkClient = new ZkClient(Config4Test.ZkConnectString, 4000);
        zkClient.deleteRecursive(ZK_BROKER_GROUP);
        File dataDir=new File("data");
        Arrays.stream(dataDir.listFiles()).forEach(f->f.delete());

        // just convenient for test
        RandomAccessBlock.BLOCK_SIZE=Config4Test.blockSize4Test;
        BasicServer basicServer=new BasicServer();
        basicServer.startup(Config4Test.configPath);
        basicServer.registerHandler(RequestHandler.FETCH,new FetchRequestHandler());
        basicServer.registerHandler(RequestHandler.PRODUCER,new ProducerRequestHandler());
        try {
            basicServer.waitForClose();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }






}
