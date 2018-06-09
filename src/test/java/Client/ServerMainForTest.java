package Client;

import com.yudy.heze.server.BasicServer;
import com.yudy.heze.server.RequestHandler;
import com.yudy.heze.server.handlers.FetchRequestHandler;
import com.yudy.heze.server.handlers.ProducerRequestHandler;
import com.yudy.heze.util.ZkUtils;
import org.I0Itec.zkclient.ZkClient;


import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.yudy.heze.util.ZkUtils.ZK_BROKER_GROUP;

public class ServerMainForTest {


    private static String configPath="conf/config.properties";
    private static ZkClient zkClient;
    private static String ZkConnectStr = "40.71.225.3:2181";


    public static void main(String[] args) {
        zkClient = new ZkClient(ZkConnectStr, 4000);
        zkClient.deleteRecursive(ZK_BROKER_GROUP);
        File dataDir=new File("data");
        Arrays.stream(dataDir.listFiles()).forEach(f->f.delete());

        BasicServer basicServer=new BasicServer();
        basicServer.startup(configPath);
        basicServer.registerHandler(RequestHandler.FETCH,new FetchRequestHandler());
        basicServer.registerHandler(RequestHandler.PRODUCER,new ProducerRequestHandler());
        try {
            basicServer.waitForClose();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }






}
