package Client;

import com.yudy.heze.server.BasicServer;
import com.yudy.heze.server.RequestHandler;
import com.yudy.heze.server.handlers.FetchRequestHandler;
import com.yudy.heze.server.handlers.ProducerRequestHandler;
import com.yudy.heze.util.ZkUtils;
import org.I0Itec.zkclient.ZkClient;

import java.io.File;
import java.util.Arrays;

public class ServerMainTest {

    static BasicServer basicServer;

    static String ZkConnectStr="127.0.0.1:2181";

    public static void main(String[] args) {
        File f=new File("data");
//        f.listFiles(File)
        Arrays.stream(f.listFiles((child) -> {
            return child.getName().endsWith(".umq")&&child.isFile();
        })).forEach(ff-> System.out.println(ff.getName()));
//        ZkClient zkClient=new ZkClient(ZkConnectStr,4000);
//        zkClient.deleteRecursive(ZkUtils.ZK_BROKER_GROUP);
//
//        zkClient.close();
//        basicServer=new BasicServer();
////        basicServer.
//        basicServer.startup("conf/config.properties");
//        basicServer.registerHandler(RequestHandler.FETCH,new FetchRequestHandler());
//        basicServer.registerHandler(RequestHandler.PRODUCER,new ProducerRequestHandler());
//        try {
//            basicServer.waitForClose();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
    }


}
