package com.yudy.heze.store.index;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Cleaner;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.channels.FileChannel;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

public class RandomAccessBlockIndex extends AbstractTopicQueueIndex {

    private static Logger LOGGER = LoggerFactory.getLogger(RandomAccessBlockIndex.class);


//    private final String indexName;
    private AtomicInteger totalNum;
    private ConcurrentSkipListMap<Long, Integer> offsetPosMap;
    // 用来记录后续不断添加的记录
//    private ConcurrentSkipListMap<Long,Integer> appendMap=new ConcurrentSkipListMap<>();
//    private volatile boolean loaded=false;

    //todo it need to be refactored thread safe initial
    public RandomAccessBlockIndex(String indexName,String fileDir) {
        String indexFilePath = formatIndexFilePath(indexName, fileDir);
        File file = new File(indexFilePath);
        try{
            if (file.exists()){
                this.indexFile = new RandomAccessFile(file, "rw");
                byte[] bytes = new byte[8];
                this.indexFile.read(bytes, 0, 8);
                if (!MAGIC.equals(new String(bytes))) {
                    throw new IllegalArgumentException("version mismatch");
                }
                this.totalNum=new AtomicInteger(this.indexFile.readInt());
                //use ConcurrentSkipListMap to maintain an order
                //so it will be easy to get the tail pos of one block
                this.offsetPosMap=new ConcurrentSkipListMap<>();
                for (int i=1;i<=totalNum.get();i++){
                    long offsetInBlock=this.indexFile.readLong();
                    int pos=this.indexFile.readInt();
                    offsetPosMap.put(offsetInBlock,pos);
                }
//                loaded=true;
            }
            else{
                this.indexFile = new RandomAccessFile(file, "rw");
                this.fileChannel = indexFile.getChannel();
                this.index = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, INDEX_SIZE);
                putMagic();
                this.totalNum=new AtomicInteger(0);
                this.offsetPosMap=new ConcurrentSkipListMap<>();
//                loaded=true;
            }
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    public boolean updateIndex(long offsetInBlock,int postion){
        this.offsetPosMap.put(offsetInBlock,postion);
        this.index.putLong(offsetInBlock);
        this.index.putInt(postion);
        this.totalNum.getAndIncrement();
        return true;
    }

    public int getReadPosition(int offsetInBlock){
        return offsetPosMap.get(offsetInBlock);
    }

    public int getTotalNum(){
        return this.totalNum.get();
    }

    public int getLastRecordPosition() {
        return this.offsetPosMap.lastEntry().getValue();
    }


    @Override
    public void putMagic() {
        this.index.position(0);
        this.index.put(MAGIC.getBytes());
    }

    @Override
    public int getWritePosition(){
        int lastPos=offsetPosMap.lastEntry().getValue();

        return -1;
    }

    public void putTotalNum(){
        this.index.position(MAGIC.getBytes().length);
        this.index.putInt(offsetPosMap.size());
    }







    /**----------------------------------------------------------------**/




    @Override
    public void putWritePosition(int writePosition) {
        this.index.position(WRITE_POS_OFFSET);
        this.index.putInt(writePosition);
        this.writePosition = writePosition;
    }

    @Override
    public void putWriteNum(int writeNum) {
        this.index.position(WRITE_NUM_OFFSET);
        this.index.putInt(writeNum);
        this.writeNum = writeNum;
    }

    @Override
    public void putWriteCounter(int writeCounter) {
        this.index.position(WRITE_CNT_OFFSET);
        this.index.putInt(writeCounter);
        this.writeCounter = writeCounter;
    }

    @Override
    public void putReadNum(int readNum) {
        this.index.position(READ_NUM_OFFSET);
        this.index.putInt(readNum);
        this.readNum = readNum;

    }

    @Override
    public void putReadPosition(int readPosition) {
        this.index.position(READ_POS_OFFSET);
        this.index.putInt(readPosition);
        this.readPosition = readPosition;
    }

    @Override
    public void putReadCounter(int readCounter) {
        this.index.position(READ_CNT_OFFSET);
        this.index.putInt(readCounter);
        this.readCounter = readCounter;
    }


    @Override
    public void reset() {
        int size = writeCounter - readCounter;
        putReadCounter(0);
        putWriteCounter(size);
        if (size == 0) {
            putReadPosition(0);
            putWritePosition(0);
        }
    }

    @Override
    public void sync() {
        if (index != null) {
            index.force();
        }
    }

    @Override
    public void close() {
        try {
            if (index == null) {
                return;
            }
            sync();
            AccessController.doPrivileged(new PrivilegedAction<Object>() {
                public Object run() {
                    try {
                        Method getCleanerMethod = index.getClass().getMethod("cleaner");
                        getCleanerMethod.setAccessible(true);
                        Cleaner cleaner = (Cleaner) getCleanerMethod.invoke(index);
                        cleaner.clean();
                    } catch (Exception e) {
                        LOGGER.error("close fqueue index file failed", e);
                    }
                    return null;
                }
            });
            index = null;
            fileChannel.close();
            indexFile.close();
        } catch (IOException e) {
            LOGGER.error("close fqueue index file failed", e);
        }

    }


}
