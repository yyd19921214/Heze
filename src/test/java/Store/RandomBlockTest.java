package Store;

import com.yudy.heze.store.block.BasicTopicQueueBlock;
import com.yudy.heze.store.index.RandomAccessBlockIndex;
import org.junit.*;
import org.junit.runners.MethodSorters;

import java.io.File;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class RandomBlockTest {

    static private RandomAccessBlockIndex index;

    static private String queueName="testBlock";

    static private String indexFileDir="data";

    static private String dataFileDir="data";

    static private String writeData_1="hello world";

    static private String writeData_2="second message";

    static private BasicTopicQueueBlock block;

    @BeforeClass
    public static void initIndex(){
        File f=new File("data/index_testBlock.umq");
        if (f.exists()){
            f.delete();
        }
        index=new RandomAccessBlockIndex(queueName,indexFileDir);
    }

    @Test
    public void test001_IndexBuild(){
        Assert.assertNotNull(index);
        Assert.assertTrue(index.getTotalNum()==0);
    }
//
//    @Test
//    public void
//




    @AfterClass
    public static void doClean(){
        File f=new File("data/index_testBlock.umq");
        if (f.exists()){
            f.delete();
        }
//        f=new File(BasicTopicQueueBlock.formatBlockFilePath(queueName, index.getReadNum(), dataFileDir));
//        if (f.exists()){
//            f.delete();
//        }

    }



}