package Store;

import com.yudy.heze.store.BasicTopicQueueBlock;
import com.yudy.heze.store.TopicQueueBlock;
import com.yudy.heze.store.index.BasicTopicQueueIndex;
import org.junit.*;
import org.junit.runners.MethodSorters;

import java.io.File;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class BasicBlockTest {

    static private BasicTopicQueueIndex index;

    static private String queueName="testBlock";

    static private String indexFileDir="data/index";

    static private String dataFileDir="data";

    static private String writeData_1="hello world";

    static private String writeData_2="second message";

    static private BasicTopicQueueBlock block;

    @BeforeClass
    public static void initIndex(){
        File f=new File("data/index/index_testBlock.umq");
        if (f.exists()){
            f.delete();
        }
        index=new BasicTopicQueueIndex(queueName,indexFileDir);
    }

    @Test
    public void test001_BuildBlock(){
        File f=new File(BasicTopicQueueBlock.formatBlockFilePath(queueName, index.getReadNum(), dataFileDir));
        if (f.exists()){
            f.delete();
        }
        block = new BasicTopicQueueBlock(index, BasicTopicQueueBlock.formatBlockFilePath(queueName, index.getReadNum(), dataFileDir));
    }

    @Test
    public void test002_ReadWrite(){
        block.write(writeData_1.getBytes());
        block.write(writeData_2.getBytes());
        block.write(writeData_1.getBytes());
        block.putEOF();
        block.sync();
        block.close();
        block = new BasicTopicQueueBlock(index, BasicTopicQueueBlock.formatBlockFilePath(queueName, index.getReadNum(), dataFileDir));
        byte[] readData=block.read();
        Assert.assertTrue(new String(readData).equals(writeData_1));
        readData=block.read();
        Assert.assertTrue(new String(readData).equals(writeData_2));
        readData=block.read();
        Assert.assertTrue(new String(readData).equals(writeData_1));
        Assert.assertTrue(block.eof());
        Assert.assertTrue(block.countRecord()==3);
    }

    @AfterClass
    public static void doClean(){
        File f=new File("data/index/index_testBlock.umq");
        if (f.exists()){
            f.delete();
        }
        f=new File(BasicTopicQueueBlock.formatBlockFilePath(queueName, index.getReadNum(), dataFileDir));
        if (f.exists()){
            f.delete();
        }

    }



}
