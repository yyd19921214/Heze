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

public class ServerMainTest {


    private static String configPath="conf/config.properties";


    public static void main(String[] args) {
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
