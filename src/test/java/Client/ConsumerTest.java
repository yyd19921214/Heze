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

    private String configPath="conf/config.properties";


    @Test
    public void test001_ConsumerPoll() throws InterruptedException {


        ServerConfig config=new ServerConfig(configPath);
        BasicConsumer consumer=new BasicConsumer(config,topicName);
        List<Topic> list=consumer.poll();
        list.forEach(t-> System.out.println(t.getContent()));
        consumer.close();
    }


}
