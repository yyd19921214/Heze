package Store;

import com.yudy.heze.store.block.RandomAccessBlock;
import com.yudy.heze.store.index.RandomAccessBlockIndex;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.File;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class RandomBlockTest {

    static private String indexName="index_test_0.umq";

    static private String fileDir="data";

    static private String writeData_1="hello world";

    static private String writeData_2="second message";

    private RandomAccessBlock block;

    private RandomAccessBlockIndex index;

    @BeforeClass
    public static void adjustBlockContent(){
        RandomAccessBlock.BLOCK_SIZE=64;
    }




    @Test
    public void test001_BlockBuild(){
        index=new RandomAccessBlockIndex(indexName,fileDir);
        Assert.assertNotNull(index);
        block=new RandomAccessBlock(index,fileDir);
        Assert.assertNotNull(block);
        Assert.assertTrue(block.isSpaceAvailable(16));
        index.close();
        block.close();
        doClean();

    }

    @Test
    public void test002_BlockWriteRead(){
        index=new RandomAccessBlockIndex(indexName,fileDir);
        block=new RandomAccessBlock(index,fileDir);
        long rtn1=block.write(writeData_1.getBytes());
        byte[] bytes=block.read(rtn1);
        Assert.assertTrue(new String(bytes).equals(writeData_1));
        long rtn2=block.write(writeData_2.getBytes());
        byte[] bytes2=block.read(rtn2);
        Assert.assertTrue(new String(bytes2).equals(writeData_2));
        Assert.assertFalse(block.isSpaceAvailable(64));
        index.close();
        block.close();
        // ========================================================

        index=new RandomAccessBlockIndex(indexName,fileDir);
        block=new RandomAccessBlock(index,fileDir);
        Assert.assertTrue(new String(block.read(1L)).equals(writeData_1));
        Assert.assertTrue(new String(block.read(2L)).equals(writeData_2));
        index.close();
        block.close();
        doClean();

    }

    @Test
    public void test003_BlockDup(){
        index=new RandomAccessBlockIndex(indexName,fileDir);
        block=new RandomAccessBlock(index,fileDir);
        RandomAccessBlock block2=block.duplicate();
        block.write(writeData_1.getBytes());
        byte[] bytes=block2.read(1L);
        Assert.assertTrue(new String(bytes).equals(writeData_1));
        index.close();
        block.close();
        doClean();
    }






    public void doClean(){
        File file = new File(fileDir);
        File[] child=file.listFiles();
        for (File f:child){
            f.delete();
        }
    }



}
