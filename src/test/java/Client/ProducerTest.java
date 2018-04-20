package Client;

import com.yudy.heze.client.producer.BasicProducer;
import com.yudy.heze.network.Topic;
import com.yudy.heze.server.BasicServer;
import com.yudy.heze.server.RequestHandler;
import com.yudy.heze.server.handlers.FetchRequestHandler;
import com.yudy.heze.server.handlers.ProducerRequestHandler;
import com.yudy.heze.store.BasicTopicQueue;
import com.yudy.heze.util.DataUtils;
import com.yudy.heze.util.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.junit.*;
import org.junit.runners.MethodSorters;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ProducerTest {


    static ZkClient zkClient;

    static String ZkConnectStr="127.0.0.1:2181";

    private String topicName="test-topic";

    private String topicContent="test-content_%d";

    private BasicTopicQueue topicQueue = null;

    private String fileDir="data";

    @BeforeClass
    public static void zkInit(){
        zkClient=new ZkClient(ZkConnectStr,4000);
    }

    @Test
    public void test001_Start(){
        BasicProducer producer=BasicProducer.getInstance();
        producer.init("file:///heze/conf/config.properties");
        Assert.assertTrue(!producer.serverIpMap.isEmpty());
        List<Topic> topics=new ArrayList<>();
        for (int i=1;i<=5;i++){
            Topic topic=new Topic();
            topic.setTopic(topicName);
            topic.setContent(String.format(topicContent,i));
            topics.add(topic);
        }

        Map<String,String> params=new HashMap<>();
        params.put("broker","MyServer01");
        boolean res=producer.send(topics,params);
        Assert.assertTrue(res);

        topicQueue = new BasicTopicQueue(topicName, fileDir);
        topicQueue.resetHead();
        for (int i=1;i<=5;i++){
            System.out.println(i);
            byte[] readData=topicQueue.poll();
            Assert.assertNotNull(readData);
            String readStr= (String) DataUtils.deserialize(readData);
            Assert.assertTrue(readStr.equals(String.format(topicContent,i)));
        }

    }



    @AfterClass
    public static void doClean(){
        zkClient.deleteRecursive(ZkUtils.ZK_BROKER_GROUP);
        zkClient.close();
    }

}

class ServerInThread implements Runnable{



    private static final String ZkConnectStr="127.0.0.1:2181";



    @Override
    public void run() {
        ZkClient zkClient=new ZkClient(ZkConnectStr,4000);
        zkClient.deleteRecursive(ZkUtils.ZK_BROKER_GROUP);
        zkClient.close();
        BasicServer basicServer=new BasicServer();
        basicServer.startup("conf/config.properties");
        basicServer.registerHandler(RequestHandler.FETCH,new FetchRequestHandler());
        basicServer.registerHandler(RequestHandler.PRODUCER,new ProducerRequestHandler());
        try {
            basicServer.waitForClose();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }




    }
}
