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


import java.io.File;
import java.io.IOException;
import java.util.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ProducerTest {


    static ZkClient zkClient;

    static String ZkConnectStr="127.0.0.1:2181";

    private String topicName="test-topic";

    private String topicContent="test-content_%d";

    private BasicTopicQueue topicQueue = null;

    private String fileDir="data";

    private String producerConfFile="file:///D://heze/conf/config.properties";

    @BeforeClass
    public static void zkInit(){
        zkClient=new ZkClient(ZkConnectStr,4000);
    }

    @Test
    public void test001_Start() throws InterruptedException {

        ServerInThread st=new ServerInThread();
        Thread serverThread=new Thread(st);
        serverThread.start();

        ServerInThread st2=new ServerInThread();
        st2.configPath="conf/config2.properties";
        Thread serverThread2=new Thread(st2);
        serverThread2.start();

        Thread.sleep(2000L);

        BasicProducer producer=BasicProducer.getInstance();
        producer.init(producerConfFile);
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

        params.clear();
        params.put("broker","MyServer02");
        boolean res2=producer.send(topics,params);
        Assert.assertTrue(res2);

        topicQueue = new BasicTopicQueue(topicName, fileDir);
        topicQueue.resetHead();
        for (int i=1;i<=10;i++){
            byte[] readData=topicQueue.poll();
            Assert.assertNotNull(readData);
            String readStr= (String) DataUtils.deserialize(readData);
            System.out.println(readStr);
        }
        topicQueue.close();
        st.stopNow();
        st2.stopNow();
        producer.stop();

    }

    @Test
    public void test002_ServerAutoFind() throws InterruptedException {
        ServerInThread st=new ServerInThread();
        Thread serverThread=new Thread(st);
        serverThread.start();
        Thread.sleep(2000L);


        BasicProducer producer=BasicProducer.getInstance();
        producer.init(producerConfFile);
        Assert.assertEquals(1,producer.serverIpMap.size());


        ServerInThread st2=new ServerInThread();
        st2.configPath="conf/config2.properties";
        Thread serverThread2=new Thread(st2);
        serverThread2.start();
        Thread.sleep(2000L);
        Assert.assertEquals(2,producer.serverIpMap.size());

        st.stopNow();
        Thread.sleep(2000L);
        Assert.assertEquals(1,producer.serverIpMap.size());
        st2.stopNow();
        Thread.sleep(2000L);
        Assert.assertEquals(0,producer.serverIpMap.size());
        producer.stop();

    }



    @AfterClass
    public static void doClean(){
        ZkClient zkClient=new ZkClient(ZkConnectStr,4000);
        zkClient.deleteRecursive(ZkUtils.ZK_BROKER_GROUP);
        zkClient.close();
        File f=new File("data");
        Arrays.stream(f.listFiles((child) -> {
            return child.getName().endsWith(".umq")&&child.isFile();
        })).forEach(ff-> ff.delete());
    }

}

class ServerInThread implements Runnable{



    BasicServer basicServer;
    String configPath="conf/config.properties";



    @Override
    public void run() {
        basicServer=new BasicServer();
        basicServer.startup(configPath);
        basicServer.registerHandler(RequestHandler.FETCH,new FetchRequestHandler());
        basicServer.registerHandler(RequestHandler.PRODUCER,new ProducerRequestHandler());
        try {
            basicServer.waitForClose();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void stopNow(){
        try {
            basicServer.directClose();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
