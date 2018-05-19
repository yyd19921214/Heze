package Store;

import com.yudy.heze.store.index.RandomAccessBlockIndex;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.File;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class RandomIndexTest {
    static private RandomAccessBlockIndex index;

    static private String indexName="index_test_0.umq";

    static private String fileDir="data";



    @Test
    public void test_001IndexBuild() {
        index=new RandomAccessBlockIndex(indexName,fileDir);
        Assert.assertNotNull(index);
        Assert.assertTrue(index.getTotalNum()==0);
        Assert.assertTrue(index.getLastOffset()==0);
        Assert.assertTrue(index.getLastRecordPosition()==-1);
        Assert.assertTrue(index.getIndexName().equals(indexName));
        index.close();
        doClean();
    }

    @Test
    public void test_002IndexWrite(){
        index=new RandomAccessBlockIndex(indexName,fileDir);
        long offset=index.getLastOffset()+1;
        index.updateIndex(offset,16);

        Assert.assertTrue(index.getTotalNum()==1);
        Assert.assertTrue(index.getLastOffset()==1);
        Assert.assertTrue(index.getLastRecordPosition()==16);

        index.close();

        index=new RandomAccessBlockIndex(indexName,fileDir);
        Assert.assertTrue(index.getTotalNum()==1);
        Assert.assertTrue(index.getLastOffset()==1);
        Assert.assertTrue(index.getLastRecordPosition()==16);
        index.close();
        doClean();

    }


    private void doClean(){
        String indexFilePath = fileDir + File.separator + indexName;
        File f = new File(indexFilePath);
        if (f.exists()){
            f.delete();
        }
    }


}
