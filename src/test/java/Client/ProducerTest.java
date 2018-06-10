package Client;

import com.yudy.heze.client.producer.BasicProducer;
import com.yudy.heze.config.Config;
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


    @BeforeClass
    public static void zkInit() {
        zkClient = new ZkClient(Config4Test.ZkConnectString, 4000);
    }

    @Test
    public void test001_ProduceData() throws InterruptedException {
        BasicProducer producer = BasicProducer.getInstance();
        producer.init(Config4Test.configPath);
        Assert.assertTrue(!producer.serverIpMap.isEmpty());
        List<Topic> topics = new ArrayList<>();
        for (int i = 1; i <= Config4Test.recordNum; i++) {
            Topic topic = new Topic();
            topic.setTopic(Config4Test.topicName);
            topic.setContent(String.format(Config4Test.topicContent, i));
            topics.add(topic);
        }
        Map<String, String> params = new HashMap<>();
        params.put("broker", Config4Test.ServerName);
        boolean res = producer.send(topics, params);
        Assert.assertTrue(res);
        producer.stop();
    }
}


