package com.yudy.heze.store.block;

import com.yudy.heze.store.index.RandomAccessBlockIndex;
import com.yudy.heze.store.queue.RandomAccessTopicQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Cleaner;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.security.AccessController;
import java.security.PrivilegedAction;


/**
 * Block File format is like this
 * -----------------
 * offset1
 * len1
 * contents1[]
 * offset2
 * len2
 * contents2[]
 */
public class RandomAccessBlock {

    private final Logger LOG = LoggerFactory.getLogger(RandomAccessBlock.class);

    public static int BLOCK_SIZE = 32 * 1024 * 1024;//32MB

    protected String blockFilePath;

    private RandomAccessBlockIndex index;

    private ByteBuffer byteBuffer;

    private MappedByteBuffer mappedBlock;

    private FileChannel fileChannel;

    private RandomAccessFile blockFile;


    public RandomAccessBlock(RandomAccessBlockIndex index, String fileDir) {
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

    private RandomAccessBlock(RandomAccessBlockIndex index, String blockFilePath, RandomAccessFile blockFile, FileChannel fileChannel, MappedByteBuffer mappedByteBuffer, ByteBuffer byteBuffer) {
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

    public RandomAccessBlock duplicate() {
        return new RandomAccessBlock(this.index, this.blockFilePath, this.blockFile, this.fileChannel, this.mappedBlock, this.byteBuffer);
    }

    public boolean isSpaceAvailable(int len) {
        int writePos = getNextWritePos();
        int increment = len + Integer.BYTES + Long.BYTES;//len(offset)+len(len)+len(msg);
        return BLOCK_SIZE >= increment + writePos;
    }

    public long write(byte[] bytes) {
        long lastRecordOffset = index.getLastOffset();
        long rtnOffset = lastRecordOffset + 1;
        int writePos = getNextWritePos();
        byteBuffer.position(writePos);
        byteBuffer.putLong(rtnOffset);
        byteBuffer.putInt(bytes.length);
        byteBuffer.put(bytes);
        //todo refine the detail of update index
        index.updateIndex(rtnOffset, writePos);
        return rtnOffset;
    }

    public byte[] read(long offset) {
        int pos = this.index.getReadPosition(offset);
        byteBuffer.position(pos);
        byteBuffer.getLong();
        int len = byteBuffer.getInt();
        byte[] bytes = new byte[len];
        byteBuffer.get(bytes);
        return bytes;
    }

    public void sync() {
        if (mappedBlock != null)
            mappedBlock.force();
        if (index != null)
            index.sync();
    }

    public void close() {
        try {
            if (mappedBlock == null) {
                return;
            }
            sync();
            AccessController.doPrivileged(new PrivilegedAction<Object>() {
                public Object run() {
                    try {
                        Method getCleanerMethod = mappedBlock.getClass().getMethod("cleaner");
                        getCleanerMethod.setAccessible(true);
                        Cleaner cleaner = (Cleaner) getCleanerMethod.invoke(mappedBlock);
                        cleaner.clean();
                    } catch (Exception e) {
                        LOG.error("close fqueue block file failed", e);
                    }
                    return null;
                }
            });

            mappedBlock = null;
            byteBuffer = null;
            index.close();
            fileChannel.close();
            blockFile.close();
        } catch (IOException e) {
            LOG.error("close fqueue block file failed", e);
        }
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
        String blockName = RandomAccessTopicQueue.BLOCK_PREFIX + indexName.substring(5);
        String blockFilePath = fileDir + File.separator + blockName;
        return blockFilePath;

    }

    private int getNextWritePos() {
        int lastRecordPos = index.getLastRecordPosition();
        if (lastRecordPos == -1) {
            return 0;
        } else {
            byteBuffer.position(lastRecordPos);
            byteBuffer.getLong();
            int lastRecordLen = byteBuffer.getInt();
            int writePos = lastRecordPos + Long.BYTES + Integer.BYTES + lastRecordLen;
            return writePos;

        }
    }

}
