package com.yudy.heze.store.index;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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
public class RandomAccessBlockIndex2 {

    private static Logger LOGGER = LoggerFactory.getLogger(RandomAccessBlockIndex2.class);
    private String indexName;
    private AtomicInteger totalNum;
    private ConcurrentSkipListMap<Long, Integer> offsetPosMap;

    private RandomAccessFile accessFile;
    private FileChannel fileChannel;
    private MappedByteBuffer buffer;


    public RandomAccessBlockIndex2(String indexName, String fileDir) {
        String indexFilePath = fileDir + File.separator + indexName;
        File file = new File(indexFilePath);
        offsetPosMap = new ConcurrentSkipListMap<>();
        try {
            if (file.exists()) {
                this.accessFile = new RandomAccessFile(file, "rw");
                totalNum = new AtomicInteger(accessFile.readInt());

                
//                for (int i=1;i<=totalNum.get();i++){
//                    long offsetInBlock=accessFile.readLong();
//                    int pos=accessFile.readInt();
//
//
//                }
            } else {

            }
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

}
