package com.yudy.heze.store.block;

import com.yudy.heze.store.index.TopicQueueIndex;
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

public class TopicQueueBlock {
    private final Logger LOG = LoggerFactory.getLogger(TopicQueueBlock.class);

    public static final String BLOCK_FILE_SUFFIX = ".umq";//数据文件

    private static final String BLOCK_FILE_PREFIX = "tblock";

    public static final int BLOCK_SIZE = 32 * 1024 * 1024;//32MB

    public static final int EOF = -1;

    private String blockFilePath;
    private TopicQueueIndex index;
    private ByteBuffer byteBuffer;
    private MappedByteBuffer mappedBlock;
    private FileChannel fileChannel;
    private RandomAccessFile blockFile;

    public static String formatBlockFilePath(String queueName, int fileNum, String fileBackupDir) {
        return fileBackupDir + File.separator + String.format("%s_%s_%d%s", BLOCK_FILE_PREFIX, queueName, fileNum, BLOCK_FILE_SUFFIX);
    }

    public TopicQueueBlock(String blockFilePath, TopicQueueIndex index, ByteBuffer byteBuffer, MappedByteBuffer mappedByteBuffer, FileChannel fileChannel, RandomAccessFile randomAccessFile) {
        this.blockFilePath = blockFilePath;
        this.index = index;
        this.byteBuffer = byteBuffer;
        this.mappedBlock = mappedByteBuffer;
        this.fileChannel = fileChannel;
        this.blockFile = randomAccessFile;
    }

    public TopicQueueBlock(TopicQueueIndex index, String blockFilePath) {
        this.blockFilePath = blockFilePath;
        this.index = index;
        try {
            File file = new File(blockFilePath);
            this.blockFile = new RandomAccessFile(file, "rw");
            this.fileChannel = this.blockFile.getChannel();
            this.mappedBlock = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, BLOCK_SIZE);
            this.byteBuffer = this.mappedBlock.load();
        } catch (Exception e) {
            e.printStackTrace();
            throw new IllegalArgumentException(e);
        }
    }

    public TopicQueueBlock duplicate() {
        return new TopicQueueBlock(this.blockFilePath, this.index, this.byteBuffer.duplicate(), this.mappedBlock, this.fileChannel, this.blockFile);
    }


    public void putEOF() {
        byteBuffer.position(index.getWritePosition());
        byteBuffer.putInt(EOF);
    }

    public boolean isSpaceAvailable(int len) {
        int increment = len + 4;
        int writePosition = index.getWritePosition();
        return BLOCK_SIZE >= increment + writePosition + 4;
    }

    public boolean eof() {
        int readPosition = index.getReadPosition();
        return readPosition > 0 && byteBuffer.getInt(readPosition) == EOF;
    }

    public int write(byte[] bytes) {
        int len = bytes.length;
        int writePosition = index.getWritePosition();
        int increment = len + 4;
        byteBuffer.position(writePosition);
        byteBuffer.putInt(len);
        byteBuffer.put(bytes);
        index.putWritePosition(increment + writePosition);
        index.putWriteCounter(index.getWriteCounter() + 1);
        return increment;
    }

    public byte[] read() {
        byte[] bytes;
        int readNum = index.getReadNum();
        int readPosition = index.getReadPosition();
        int writeNum = index.getWriteNum();
        int writePosition = index.getWritePosition();
        if (readNum == writeNum && readPosition >= writePosition)
            return null;
        byteBuffer.position(readPosition);
        int len = byteBuffer.getInt();
        if (len <= 0)
            return null;
        bytes = new byte[len];
        byteBuffer.get(bytes);
        index.putReadPosition(readPosition + 4 + len);
        index.putReadCounter(index.getReadCounter() + 1);
        return bytes;
    }

    public byte[] read(int readPosition) {
        byte[] bytes;
        byteBuffer.position(readPosition);
        int dataLength = byteBuffer.getInt();
        if (dataLength <= 0)
            return null;
        bytes = new byte[dataLength];
        byteBuffer.get(bytes);
        return bytes;

    }

    public void sync() {
        if (mappedBlock != null)
            mappedBlock.force();
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
            fileChannel.close();
            blockFile.close();
        } catch (IOException e) {
            LOG.error("close fqueue block file failed", e);
        }
    }

    public String getBlockFilePath(){return this.blockFilePath;}

    public int count() {
        return 0;
    }


}
