package com.yudy.heze.store.index;

import com.yudy.heze.store.TopicQueueIndex;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

abstract public class AbstractTopicQueueIndex implements TopicQueueIndex {

    protected final String INDEX_FILE_SUFFIX = ".umq";

    protected final String INDEX_FILE_PREFIX = "index";

    protected volatile int readNum;        // 8   读索引文件号
    protected volatile int readPosition;   // 12  读索引位置
    protected volatile int readCounter;    // 16  总读取数量
    protected volatile int writeNum;       // 20  写索引文件号
    protected volatile int writePosition;  // 24  写索引位置
    protected volatile int writeCounter;   // 28  总写入数量

    protected RandomAccessFile indexFile;
    protected FileChannel fileChannel;
    protected MappedByteBuffer index;

    protected boolean isIndexFile(String fileName) {
        return fileName.endsWith(INDEX_FILE_SUFFIX);
    }

    protected String formatIndexFilePath(String queueName, String fileDir) {
        return fileDir + File.separator + String.format("%s_%s%s", INDEX_FILE_PREFIX, queueName, INDEX_FILE_SUFFIX);
    }

    @Override
    public int getReadNum() {
        return this.readNum;
    }

    @Override
    public int getReadPosition() {
        return this.readPosition;
    }

    @Override
    public int getReadCounter() {
        return this.readCounter;
    }

    @Override
    public int getWriteNum() {
        return this.writeNum;
    }

    @Override
    public int getWritePosition() {
        return this.writePosition;
    }

    @Override
    public int getWriteCounter() {
        return this.writeCounter;
    }




}
