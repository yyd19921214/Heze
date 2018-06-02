package Client;

import com.yudy.heze.client.producer.BasicProducer;
import com.yudy.heze.network.Topic;
import com.yudy.heze.server.BasicServer;
import com.yudy.heze.server.RequestHandler;
import com.yudy.heze.server.handlers.FetchRequestHandler;
import com.yudy.heze.server.handlers.ProducerRequestHandler;
import com.yudy.heze.store.queue.BasicTopicQueue;
import com.yudy.heze.store.queue.RandomAccessTopicQueue;
import com.yudy.heze.util.DataUtils;
import com.yudy.heze.util.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.junit.*;
import org.junit.runners.MethodSorters;


import java.io.File;
import java.io.IOException;
import java.util.*;

import static com.yudy.heze.util.ZkUtils.ZK_BROKER_GROUP;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ProducerTest {


    static ZkClient zkClient;

    static String ZkConnectStr = "40.71.225.3:2181";

    private String topicName = "test-topic";

    private String topicContent = "test-content_%d";

    private RandomAccessTopicQueue topicQueue = null;

    private String fileDir = "data";

    private String producerConfFile = "file:///C://opensource/Heze/conf/config.properties";

    @BeforeClass
    public static void zkInit() {
        zkClient = new ZkClient(ZkConnectStr, 4000);
    }

    @Test
    public void test001_Start() throws InterruptedException {


        zkClient.deleteRecursive(ZK_BROKER_GROUP);

        ServerInThread st = new ServerInThread();
        Thread serverThread = new Thread(st);
        serverThread.start();


        Thread.sleep(2000L);

        BasicProducer producer = BasicProducer.getInstance();
        producer.init(producerConfFile);
        Assert.assertTrue(!producer.serverIpMap.isEmpty());
        List<Topic> topics = new ArrayList<>();
        for (int i = 1; i <= 5; i++) {
            Topic topic = new Topic();
            topic.setTopic(topicName);
            topic.setContent(String.format(topicContent, i));
            topics.add(topic);
        }

        Map<String, String> params = new HashMap<>();
        params.put("broker", "MyServer01");
        boolean res = producer.send(topics, params);

        Assert.assertTrue(res);
        st.stopNow();
        producer.stop();

    }

    @Test
    public void test002_CheckData(){
        topicQueue = new RandomAccessTopicQueue(topicName, fileDir);
        for (int i = 1; i <= 5; i++) {
            byte[] readData = topicQueue.read(i);
            Assert.assertNotNull(readData);
            String readStr = (String) DataUtils.deserialize(readData);
            System.out.println(readStr);
        }
        topicQueue.close();
    }


    class ServerInThread implements Runnable {

        BasicServer basicServer;
        String configPath = "conf/config.properties";


        @Override
        public void run() {
            basicServer = new BasicServer();
            basicServer.startup(configPath);
            basicServer.registerHandler(RequestHandler.FETCH, new FetchRequestHandler());
            basicServer.registerHandler(RequestHandler.PRODUCER, new ProducerRequestHandler());
            try {
                basicServer.waitForClose();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        public void stopNow() {
            try {
                basicServer.directClose();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @AfterClass
    public static void doClean(){
        File f=new File("data");
        Arrays.stream(f.listFiles((child) -> child.getName().endsWith(".umq")&&child.isFile()
        )).forEach(ff-> ff.delete());
    }


}


