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

public class BasicTopicQueueIndex extends AbstractTopicQueueIndex implements TopicQueueIndex{

    private static final Logger LOGGER = LoggerFactory.getLogger(BasicTopicQueueIndex.class);

    public BasicTopicQueueIndex(String queueName, String fileDir) {
        String indexFilePath = formatIndexFilePath(queueName, fileDir);
        File file = new File(indexFilePath);
        try {
            if (file.exists()) {
                this.indexFile = new RandomAccessFile(file, "rw");
                byte[] bytes = new byte[8];
                this.indexFile.read(bytes, 0, 8);
                if (!MAGIC.equals(new String(bytes))) {
                    throw new IllegalArgumentException("version mismatch");
                }

                this.readNum = indexFile.readInt();
                this.readPosition = indexFile.readInt();
                this.readCounter = indexFile.readInt();

                this.writeNum = indexFile.readInt();
                this.writePosition = indexFile.readInt();
                this.writeCounter = indexFile.readInt();

                this.fileChannel = indexFile.getChannel();
                this.index = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, INDEX_SIZE);
                this.index = index.load();

            } else {
                this.indexFile = new RandomAccessFile(file, "rw");
                this.fileChannel = indexFile.getChannel();
                this.index = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, INDEX_SIZE);

                putMagic();
                putReadNum(0);
                putReadPosition(0);
                putReadCounter(0);

                putWriteNum(0);
                putWritePosition(0);
                putWriteCounter(0);
            }
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }


    @Override
    public void putMagic() {
        this.index.position(0);
        this.index.put(MAGIC.getBytes());

    }

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
            index.position(0);
            StringBuilder sb = new StringBuilder();
            byte[] bytes = new byte[8];
            index.get(bytes, 0, 8);
            sb.append("disk index").append("=>").append("readNum:").append(index.getInt())
                    .append(",readPosition:").append(index.getInt())
                    .append(",readCounter:").append(index.getInt())
                    .append(",writeNum:").append(index.getInt())
                    .append(",writePosition:").append(index.getInt())
                    .append(",writeCounter:").append(index.getInt());
            LOGGER.info(sb.toString());
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
