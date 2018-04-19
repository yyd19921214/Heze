package Client;

import com.yudy.heze.client.producer.BasicProducer;
import com.yudy.heze.network.Topic;
import com.yudy.heze.server.BasicServer;
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

//    @Test
    public void test002_CheckProducer(){
        topicQueue = new BasicTopicQueue(topicName, fileDir);
        topicQueue.resetHead();
        System.out.println(topicQueue.index.getReadCounter());
        System.out.println(topicQueue.index.getReadNum());
        System.out.println(topicQueue.index.getReadPosition());
        byte[] readData=topicQueue.poll();
        System.out.println(DataUtils.deserialize(readData));
//        System.out.println("1111"+l.get(0));
        Assert.assertNotNull(readData);
    }

    @AfterClass
    public static void doClean(){
        zkClient.deleteRecursive(ZkUtils.ZK_BROKER_GROUP);
        zkClient.close();
    }








}
