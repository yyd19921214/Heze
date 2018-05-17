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

class A{

}
class B extends A{

}
public class ServerMainTest {

    public void fun(List<? extends A> l){

    }

    static BasicServer basicServer;

    static String ZkConnectStr="127.0.0.1:2181";

    public static void main(String[] args) {
        A a=new A();
        B b=new B();
        List<B> l=new ArrayList();
        l.add(b);
        ServerMainTest smt=new ServerMainTest();



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
