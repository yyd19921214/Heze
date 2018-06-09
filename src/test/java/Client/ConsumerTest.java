package Client;

import com.yudy.heze.client.consumer.BasicConsumer;
import com.yudy.heze.client.producer.BasicProducer;
import com.yudy.heze.config.ServerConfig;
import com.yudy.heze.network.Topic;
import com.yudy.heze.server.BasicServer;
import com.yudy.heze.server.RequestHandler;
import com.yudy.heze.server.handlers.FetchRequestHandler;
import com.yudy.heze.server.handlers.ProducerRequestHandler;
import org.I0Itec.zkclient.ZkClient;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.yudy.heze.util.ZkUtils.ZK_BROKER_GROUP;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ConsumerTest {

    static ZkClient zkClient;

    @BeforeClass
    public static void zkInit() {
        zkClient = new ZkClient(ZkConnectStr, 4000);
    }
    static String ZkConnectStr = "40.71.225.3:2181";

    private String topicName = "test-topic";
    private String topicContent = "test-content_%d";

    private String producerConfFile = "file:///C://opensource/Heze/conf/config.properties";

    private String configPath="conf/config.properties";


    @Test
    public void test001_ConsumerPoll() throws InterruptedException {

        zkClient.deleteRecursive(ZK_BROKER_GROUP);
//        ServerInThread st=new ServerInThread();
//        Thread serverThread = new Thread(st);
//        serverThread.start();
//        Thread.sleep(2000L);

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

        ServerConfig config=new ServerConfig(configPath);
        BasicConsumer consumer=new BasicConsumer(config,topicName);
        List<Topic> list=consumer.poll();
        list.forEach(t-> System.out.println(t.getContent()));

//        st.stopNow();
        producer.stop();
        consumer.close();


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
}
