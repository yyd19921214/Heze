package com.yudy.heze.store.index;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Cleaner;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * the structure of index is below
 * --------------------------
 * <p>
 * total number of message in this index  int
 * message1 offset  long
 * message1 position  int
 * message2 offset  long
 * message2 position  int
 * message3 offset  long
 * message3 position  int
 * ---------------------------
 */
public class RandomAccessBlockIndex {

    private static Logger LOGGER = LoggerFactory.getLogger(RandomAccessBlockIndex.class);
    public static final int INDEX_SIZE = 1*1024*1024;

    private String indexName;
    private AtomicInteger totalNum;
    private ConcurrentSkipListMap<Long, Integer> offsetPosMap;
    private long startFromOffset;

    private RandomAccessFile accessFile;
    private FileChannel fileChannel;
    private MappedByteBuffer buffer;


    public RandomAccessBlockIndex(String indexName, String fileDir) {
        this.indexName = indexName;
        startFromOffset=extractStartOffset(indexName);
        String indexFilePath = fileDir + File.separator + indexName;
        File file = new File(indexFilePath);
        offsetPosMap = new ConcurrentSkipListMap<>();
        try {
            if (file.exists()) {
                accessFile = new RandomAccessFile(file, "rw");
                totalNum = new AtomicInteger(accessFile.readInt());
                for (int i = 1; i <= totalNum.get(); i++) {
                    long offset = accessFile.readLong();
                    int position = accessFile.readInt();
                    offsetPosMap.put(offset, position);
                }
                fileChannel = accessFile.getChannel();
                buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, INDEX_SIZE);
                buffer = buffer.load();
            } else {
                accessFile = new RandomAccessFile(file, "rw");
                fileChannel = accessFile.getChannel();
                buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, INDEX_SIZE);
                buffer.position(0);
                buffer.putInt(0);
                totalNum = new AtomicInteger(0);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public boolean updateIndex(long offset, int position) {
        if (offsetPosMap.containsKey(offset)) {
            throw new IllegalArgumentException("offset has been write");
        }
        offsetPosMap.put(offset, position);

        buffer.putLong(offset);
        buffer.putInt(position);
        totalNum.incrementAndGet();
        return true;
    }

    public int getReadPosition(long offset) {
        if (!offsetPosMap.containsKey(offset)){
            throw new IllegalArgumentException(String.format("offset %d not existed",offset));
        }
        return offsetPosMap.get(offset);
    }

    public int getTotalNum() {
        return this.totalNum.get();
    }

    public int getLastRecordPosition() {
        if (!offsetPosMap.isEmpty()){
            return offsetPosMap.lastEntry().getValue();
        }
        return -1;
    }

    public long getLastOffset() {
        if (!offsetPosMap.isEmpty()){
            return offsetPosMap.lastKey();
        }
        else{
            return startFromOffset;
        }
    }

    public String getIndexName() {
        return indexName;
    }

    public void sync() {
        if (buffer != null) {
            buffer.position(0);
            buffer.putInt(totalNum.get());
            buffer.force();
        }

    }

    public void close(){
        try {
            if (buffer == null) {
                return;
            }
            sync();
            AccessController.doPrivileged(new PrivilegedAction<Object>() {
                public Object run() {
                    try {
                        Method getCleanerMethod = buffer.getClass().getMethod("cleaner");
                        getCleanerMethod.setAccessible(true);
                        Cleaner cleaner = (Cleaner) getCleanerMethod.invoke(buffer);
                        cleaner.clean();
                    } catch (Exception e) {
                        LOGGER.error("close fqueue index file failed", e);
                    }
                    return null;
                }
            });
            buffer = null;
            fileChannel.close();
            accessFile.close();
        } catch (IOException e) {
            LOGGER.error("close fqueue index file failed", e);
        }

    }

    private long extractStartOffset(String indexName){
        String[] fields=indexName.split("_");
        String offsetPart=fields[fields.length-1];
        offsetPart=offsetPart.replace(".umq","");
        return Long.parseLong(offsetPart);
    }


}
