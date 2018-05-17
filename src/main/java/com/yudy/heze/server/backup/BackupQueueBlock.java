package com.yudy.heze.server.backup;

import com.yudy.heze.store.index.DiskTopicQueueIndex;
import com.yudy.heze.store.index.ZkTopicQueueReadIndex;
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

import static com.yudy.heze.store.block.TopicQueueBlock.BLOCK_FILE_SUFFIX;
import static com.yudy.heze.store.block.TopicQueueBlock.BLOCK_SIZE;
import static com.yudy.heze.store.block.TopicQueueBlock.EOF;


public class BackupQueueBlock {

    private final Logger LOGGER = LoggerFactory.getLogger(BackupQueueBlock.class);

    private String blockFilePath;
    private DiskTopicQueueIndex writeIndex;
    private ZkTopicQueueReadIndex readIndex;
    private ByteBuffer byteBuffer;
    private MappedByteBuffer mappedBlock;
    private FileChannel fileChannel;
    private RandomAccessFile blockFile;

    public BackupQueueBlock(String blockFilePath, DiskTopicQueueIndex writeIndex, ZkTopicQueueReadIndex readIndex, RandomAccessFile blockFile, FileChannel fileChannel,
                            ByteBuffer byteBuffer, MappedByteBuffer mappedBlock) {
        this.blockFilePath = blockFilePath;
        this.writeIndex = writeIndex;
        this.readIndex = readIndex;
        this.blockFile = blockFile;
        this.fileChannel = fileChannel;
        this.mappedBlock = mappedBlock;
        this.byteBuffer = byteBuffer;

    }

    public BackupQueueBlock(DiskTopicQueueIndex writeIndex, ZkTopicQueueReadIndex readIndex, String blockFilePath){
        this.writeIndex = writeIndex;
        this.readIndex = readIndex;
        this.blockFilePath = blockFilePath;
        try {
            File file = new File(blockFilePath);
            this.blockFile = new RandomAccessFile(file, "rw");
            this.fileChannel = blockFile.getChannel();
            this.mappedBlock = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, BLOCK_SIZE);
            this.byteBuffer = mappedBlock.load();
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }

    public BackupQueueBlock duplicate() {
        return new BackupQueueBlock(this.blockFilePath, this.writeIndex, this.readIndex, this.blockFile, this.fileChannel,
                this.byteBuffer.duplicate(), this.mappedBlock);
    }

    public static String formatBlockFilePath(String queueName, int fileNum, String fileBackupDir) {
        return fileBackupDir + File.separator + String.format("tblock_%s_%d%s", queueName, fileNum, BLOCK_FILE_SUFFIX);
    }

    public String getBlockFilePath() {
        return blockFilePath;
    }

    public void putEOF() {
        this.byteBuffer.position(writeIndex.getWritePosition());
        this.byteBuffer.putInt(EOF);
    }

    public boolean isSpaceAvailable(int len) {
        int increment = len + 4;
        int writePosition = writeIndex.getWritePosition();
        return BLOCK_SIZE >= increment + writePosition + 4;
    }

    public boolean eof() {
        int readPosition = readIndex.getReadPosition();
        return readPosition > 0 && byteBuffer.getInt(readPosition) == EOF;
    }

    public int write(byte[] bytes) {
        int len = bytes.length;
        int increment = len + 4;
        int writePosition = writeIndex.getWritePosition();
        byteBuffer.position(writePosition);
        byteBuffer.putInt(len);
        byteBuffer.put(bytes);
        writeIndex.putWritePosition(increment + writePosition);
        writeIndex.putWriteCounter(writeIndex.getWriteCounter() + 1);
        return increment;
    }

    public byte[] read() {
        byte[] bytes;
        int readNum = readIndex.getReadNum();
        int readPosition = readIndex.getReadPosition();
        int writeNum = writeIndex.getWriteNum();
        int writePosition = writeIndex.getWritePosition();
        if (readNum == writeNum && readPosition >= writePosition) {
            return null;
        }
        byteBuffer.position(readPosition);
        int dataLength = byteBuffer.getInt();
        if (dataLength <= 0) {
            return null;
        }
        bytes = new byte[dataLength];
        byteBuffer.get(bytes);
        readIndex.putReadPosition(readPosition + bytes.length + 4);
        readIndex.putReadCounter(readIndex.getReadCounter() + 1);
        readIndex.activeSyncForRead();//check again
        return bytes;
    }

    public void sync() {
        if (mappedBlock != null) {
            mappedBlock.force();
        }
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
                        LOGGER.error("close fqueue block file failed", e);
                    }
                    return null;
                }
            });
            mappedBlock = null;
            byteBuffer = null;
            fileChannel.close();
        } catch (IOException e) {
            LOGGER.error("close fqueue block file failed", e);
        }
    }


}
