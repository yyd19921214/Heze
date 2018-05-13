package com.yudy.heze.store.block;

import com.yudy.heze.store.index.RandomAccessBlockIndex;
import com.yudy.heze.store.index.TopicQueueIndex;
import com.yudy.heze.store.queue.RandomAccessTopicQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class RandomAccessBlock2 {

    private final Logger LOG = LoggerFactory.getLogger(RandomAccessBlock2.class);

    public static int BLOCK_SIZE = 32 * 1024 * 1024;//32MB

    protected String blockFilePath;

    private RandomAccessBlockIndex index;

    private ByteBuffer byteBuffer;

    private MappedByteBuffer mappedBlock;

    private FileChannel fileChannel;

    private RandomAccessFile blockFile;


    public RandomAccessBlock2(RandomAccessBlockIndex index, String fileDir) {
        this.index = index;
        this.blockFilePath = getBlockFilePath(index.getIndexName(), fileDir);
        try {
            File file = new File(this.blockFilePath);
            this.blockFile = new RandomAccessFile(file, "rw");
            this.fileChannel = this.blockFile.getChannel();
            this.mappedBlock = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, BLOCK_SIZE);
            this.byteBuffer = this.mappedBlock.load();
        } catch (Exception e) {
            e.printStackTrace();
            throw new IllegalArgumentException(e);
        }
    }

    private RandomAccessBlock2(RandomAccessBlockIndex index, String blockFilePath, RandomAccessFile blockFile, FileChannel fileChannel, MappedByteBuffer mappedByteBuffer, ByteBuffer byteBuffer) {
        this.index = index;
        this.blockFilePath = blockFilePath;
        this.blockFile = blockFile;
        this.fileChannel = fileChannel;
        this.mappedBlock = mappedByteBuffer;
        this.byteBuffer = byteBuffer;
    }

    public RandomAccessBlockIndex getIndex() {
        return this.index;
    }

    public RandomAccessBlock2 duplicate() {
        return new RandomAccessBlock2(this.index, this.blockFilePath, this.blockFile, this.fileChannel, this.mappedBlock, this.byteBuffer);
    }


    /**
     * the index name is like: index_{queueName}_{first offset of this index}.umq
     * the block name if like: block_{queueName}_{first offset of this block}.umq
     *
     * @param indexName
     * @param fileDir
     * @return
     */
    private String getBlockFilePath(String indexName, String fileDir) {
        String blockName = RandomAccessTopicQueue.BLOCK_PREFIX + indexName.substring(4);
        String blockFilePath = fileDir + File.separator + blockName;
        return blockFilePath;

    }

}
