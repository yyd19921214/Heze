package com.yudy.heze.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;

/**
 * 一种支持数据随机访问的文件结构
 * 利用Index完成对数据的定位
 * 具体实现见链接: https://tech.meituan.com/kafka-fs-design-theory.html
 */
public class RandomAccessBlock extends AbstractQueueBlock{

    private final Logger LOG = LoggerFactory.getLogger(RandomAccessBlock.class);

    public static final String BLOCK_FILE_SUFFIX = ".umq";//数据文件

    private static final String BLOCK_FILE_PREFIX = "block";

    public static int BLOCK_SIZE = 32 * 1024 * 1024;//32MB

    private String blockFilePath;
    private TopicQueueIndex index;
    private ByteBuffer byteBuffer;
    private MappedByteBuffer mappedBlock;
    private FileChannel fileChannel;
    private RandomAccessFile blockFile;


    public RandomAccessBlock(String blockFilePath, TopicQueueIndex index, ByteBuffer byteBuffer, MappedByteBuffer mappedByteBuffer, FileChannel fileChannel, RandomAccessFile randomAccessFile) {
        super(blockFilePath,index,byteBuffer,mappedByteBuffer,fileChannel,randomAccessFile);
    }

    public RandomAccessBlock(TopicQueueIndex index, String blockFilePath) {
        super(index,blockFilePath);
    }

    @Override
    public RandomAccessBlock duplicate() {
        return new RandomAccessBlock(this.blockFilePath, this.index, this.byteBuffer.duplicate(), this.mappedBlock, this.fileChannel, this.blockFile);
    }

    @Override
    public void putEOF() {
        byteBuffer.position(index.getWritePosition());
        byteBuffer.putInt(EOF);
    }

    @Override
    public boolean isSpaceAvailable(int len) {
        int increment = len + Integer.BYTES +Long.BYTES;//len(msg)+len(len)+len(offset);
        int writePosition = index.getWritePosition();
        return BLOCK_SIZE >= increment + writePosition + 4;
    }

    @Override
    public boolean eof() {
        int readPosition = index.getReadPosition();
        return readPosition > 0 && byteBuffer.getInt(readPosition) == EOF;
    }

    @Override
    public int write(byte[] bytes) {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] read() {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] read(int readPos) {
//        index.getReadPosition()
    }

    @Override
    public int countRecord() {
        return 0;
    }

    @Override
    public void sync() {

    }

    @Override
    public void close() {

    }


}
