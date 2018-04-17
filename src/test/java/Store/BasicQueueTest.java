package Store;

import com.yudy.heze.store.BasicTopicQueue;
import com.yudy.heze.store.BasicTopicQueueBlock;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.File;
import java.util.Arrays;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class BasicQueueTest {

    static private String queueName = "basicQueue";

    static private String fileDir = "data";

    private BasicTopicQueue topicQueue = null;

    @BeforeClass
    public static void InitQueue() {
        BasicTopicQueueBlock.BLOCK_SIZE = 128;

    }


    //测试发送少量消息，不涉及block的重新分配
    @Test
    public void test002_ReadWrite() {
        topicQueue = new BasicTopicQueue(queueName, fileDir);
        String writeData = "Hello World_%d";
        for (int i = 1; i <= 5; i++) {
            topicQueue.offer(String.format(writeData, i).getBytes());
        }
        Assert.assertTrue(topicQueue.size() == 5);
        for (int i = 1; i <= 5; i++) {
            Assert.assertTrue(new String(topicQueue.poll()).equals(String.format(writeData, i)));
        }
        Assert.assertTrue(topicQueue.poll() == null);
        Assert.assertTrue(topicQueue.index.getWriteNum() == 0);
        Assert.assertTrue(topicQueue.index.getReadNum() == 0);
        topicQueue.sync();
        topicQueue.close();
        topicQueue = new BasicTopicQueue(queueName, fileDir);
        topicQueue.resetHead();
        for (int i = 1; i <= 5; i++) {
            Assert.assertTrue(new String(topicQueue.poll()).equals(String.format(writeData, i)));
        }
        topicQueue.locate(3);
        String readData = new String(topicQueue.poll());
        Assert.assertTrue(readData.equals("Hello World_3"));
        topicQueue.skip(1);
        readData = new String(topicQueue.poll());
        Assert.assertTrue(readData.equals("Hello World_5"));
        topicQueue.skip(-1);
        readData = new String(topicQueue.poll());
        Assert.assertTrue(readData.equals("Hello World_5"));
        topicQueue.close();
        doClean();
    }

    //测试发送较多消息，设计block的重新分配
    @Test
    public void test003_ReadWrite() {
        topicQueue = new BasicTopicQueue(queueName, fileDir);
        String writeData = "Good Night_%d";
        for (int i = 1; i <= 40; i++) {
            topicQueue.offer(String.format(writeData, i).getBytes());
        }
        Assert.assertTrue(topicQueue.index.getWriteCounter() == 40);
        Assert.assertTrue(topicQueue.index.getWriteNum() == 5);
        for (int i = 1; i <= 40; i++) {
            Assert.assertTrue(new String(topicQueue.poll()).equals(String.format(writeData, i)));
        }
        topicQueue.resetHead();
        String readData;
        Assert.assertTrue(topicQueue.index.getReadPosition() == 0);
        topicQueue.locate(23);
        readData = new String(topicQueue.poll());
        Assert.assertTrue(readData.equals(String.format(writeData, 23)));
        topicQueue.locate(1);
        readData = new String(topicQueue.poll());
        Assert.assertTrue(readData.equals(String.format(writeData, 1)));
        topicQueue.locate(29);
        topicQueue.skip(-12);
        readData = new String(topicQueue.poll());
        Assert.assertTrue(readData.equals(String.format(writeData, 17)));
        topicQueue.skip(1);
        readData = new String(topicQueue.poll());
        Assert.assertTrue(readData.equals(String.format(writeData, 19)));

        topicQueue.close();

        topicQueue = new BasicTopicQueue(queueName, fileDir);
        topicQueue.resetHead();
        topicQueue.locate(19);
        readData = new String(topicQueue.poll());
        Assert.assertTrue(readData.equals(String.format(writeData, 19)));
        topicQueue.locate(6);
        topicQueue.skip(4);
        readData = new String(topicQueue.poll());
        Assert.assertTrue(readData.equals(String.format(writeData, 10)));

        doClean();
    }

    private void doClean() {
        topicQueue.close();
        File f = new File(fileDir);
        Arrays.stream(f.listFiles(File::isFile)).forEach(File::delete);
    }


}
