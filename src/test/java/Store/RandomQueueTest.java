package Store;

import com.yudy.heze.store.block.BasicTopicQueueBlock;
import com.yudy.heze.store.block.RandomAccessBlock;
import com.yudy.heze.store.queue.BasicTopicQueue;
import com.yudy.heze.store.queue.RandomAccessTopicQueue;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.File;
import java.util.Arrays;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class RandomQueueTest {

    static private String queueName = "basicQueue";

    static private String fileDir = "data";

    private RandomAccessTopicQueue topicQueue = null;

    @BeforeClass
    public static void InitQueue() {
        RandomAccessBlock.BLOCK_SIZE = 256;

    }

    //测试发送少量消息，不涉及block的重新分配
    @Test
    public void test002_ReadWrite() {
        topicQueue = new RandomAccessTopicQueue(queueName, fileDir);
        String writeData = "Hello World_%d";
        for (int i = 1; i <= 5; i++) {
            topicQueue.append(String.format(writeData, i).getBytes());
        }
        for (int i = 1; i <= 5; i++) {
            Assert.assertTrue(new String(topicQueue.read(i)).equals(String.format(writeData, i)));
        }
        topicQueue.close();

        topicQueue = new RandomAccessTopicQueue(queueName, fileDir);
        for (int i = 1; i <= 5; i++) {
            Assert.assertTrue(new String(topicQueue.read(i)).equals(String.format(writeData, i)));
        }
        for (int i = 6; i <= 10; i++) {
            topicQueue.append(String.format(writeData, i).getBytes());
        }
        for (int i = 1; i <= 10; i++) {
            Assert.assertTrue(new String(topicQueue.read(i)).equals(String.format(writeData, i)));
        }
        topicQueue.close();
        doClean();
    }

    //测试发送较多消息，涉及block的重新分配
    @Test
    public void test003_ReadWrite() {
        topicQueue = new RandomAccessTopicQueue(queueName, fileDir);
        String writeData = "Good Night_%d";
        for (int i = 1; i <= 40; i++) {
            topicQueue.append(String.format(writeData, i).getBytes());
        }
        for (int i = 1; i <= 40; i++) {
            Assert.assertTrue(new String(topicQueue.read(i)).equals(String.format(writeData, i)));
        }
        topicQueue.close();

//        topicQueue = new RandomAccessTopicQueue(queueName, fileDir);
//        for (int i = 41; i <= 60; i++) {
//            topicQueue.append(String.format(writeData, i).getBytes());
//        }
//        for (int i = 1; i <= 60; i++) {
//            Assert.assertTrue(new String(topicQueue.read(i)).equals(String.format(writeData, i)));
//        }
//        topicQueue.close();
        doClean();
    }



    private void doClean() {
        topicQueue.close();
        File f = new File(fileDir);
        Arrays.stream(f.listFiles(File::isFile)).forEach(File::delete);
    }
}
