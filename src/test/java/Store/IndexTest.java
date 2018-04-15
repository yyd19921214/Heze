package Store;

import com.yudy.heze.store.index.BasicTopicQueueIndex;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.File;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class IndexTest {

    static private BasicTopicQueueIndex index;

    static private String queueName="testQueue";

    static private String fileDir="data/index";

    @BeforeClass
    public static void initIndex(){
        File f=new File("data/index/index_testQueue.umq");
        if (f.exists()){
            f.delete();
        }
        index=new BasicTopicQueueIndex(queueName,fileDir);
    }

    @Test
    public void test_001IndexBuild(){
        Assert.assertNotNull(index);
        Assert.assertTrue(index.getWritePosition()==0);
        Assert.assertTrue(index.getWriteCounter()==0);
        Assert.assertTrue(index.getWriteNum()==0);
        Assert.assertTrue(index.getReadPosition()==0);
        Assert.assertTrue(index.getReadCounter()==0);
        Assert.assertTrue(index.getReadNum()==0);

    }

    @Test
    public void test_002IndexReadWrite(){
        index.putMagic();
        index.putWriteCounter(1);
        index.putWritePosition(5);
        index.putWriteNum(10);
        index.putReadCounter(0);
        index.putReadNum(4);
        index.putReadPosition(8);
        index.sync();
        index.close();
        index=new BasicTopicQueueIndex(queueName,fileDir);
        Assert.assertTrue(index.getWriteCounter()==1);
        Assert.assertTrue(index.getWritePosition()==5);
        Assert.assertTrue(index.getWriteNum()==10);
        Assert.assertTrue(index.getReadCounter()==0);
        Assert.assertTrue(index.getReadNum()==4);
        Assert.assertTrue(index.getReadPosition()==8);
        index.reset();
        Assert.assertTrue(index.getWriteCounter()==1);
    }



}
