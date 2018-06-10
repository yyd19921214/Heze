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
        zkClient = new ZkClient(Config4Test.ZkConnectString, 4000);
    }



    @Test
    public void test001_ConsumerPoll() throws InterruptedException {

        ServerConfig config=new ServerConfig(Config4Test.configPath);
        BasicConsumer consumer=new BasicConsumer(config,Config4Test.topicName);

        //int preNum=Config4Test.recordNum/2;
        List<Topic> list=consumer.poll(Config4Test.recordNum);
        Assert.assertNotNull(list);
        Assert.assertTrue(list.stream().allMatch(t->t.getContent().equals(String.format(Config4Test.topicContent,t.getReadOffset()))));
        Assert.assertTrue(consumer.getNextOffset()==Config4Test.recordNum+1);
        consumer.skipTo(50);
        list=consumer.poll();
        Assert.assertNotNull(list);
        Assert.assertTrue(list.get(0).getReadOffset()==50);
        Assert.assertTrue(list.stream().allMatch(t->t.getContent().equals(String.format(Config4Test.topicContent,t.getReadOffset()))));

        consumer.close();
    }


}
