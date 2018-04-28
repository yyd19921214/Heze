package com.yudy.heze.store.block;

import com.yudy.heze.store.index.TopicQueueIndex;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

abstract class AbstractQueueBlock {

    public static final String BLOCK_FILE_SUFFIX = ".umq";//数据文件

    private static final String BLOCK_FILE_PREFIX = "block";

    public static int BLOCK_SIZE = 32 * 1024 * 1024;//32MB

    public static final int EOF = -1;

    protected String blockFilePath;

    private TopicQueueIndex index;

    private ByteBuffer byteBuffer;

    private MappedByteBuffer mappedBlock;

    private FileChannel fileChannel;

    private RandomAccessFile blockFile;

    public static String formatBlockFilePath(String queueName, int fileNum, String fileBackupDir) {
        return fileBackupDir + File.separator + String.format("%s_%s_%d%s", BLOCK_FILE_PREFIX, queueName, fileNum, BLOCK_FILE_SUFFIX);
    }

    public AbstractQueueBlock(String blockFilePath, TopicQueueIndex index, ByteBuffer byteBuffer, MappedByteBuffer mappedByteBuffer, FileChannel fileChannel, RandomAccessFile randomAccessFile) {
        this.blockFilePath = blockFilePath;
        this.index = index;
        this.byteBuffer = byteBuffer;
        this.mappedBlock = mappedByteBuffer;
        this.fileChannel = fileChannel;
        this.blockFile = randomAccessFile;
    }

    public AbstractQueueBlock(TopicQueueIndex index, String blockFilePath) {
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

//    public AbstractQueueBlock duplicate(){
//        return new AbstractQueueBlock(this.blockFilePath, this.index, this.byteBuffer.duplicate(), this.mappedBlock, this.fileChannel, this.blockFile);
//    }

    abstract public void putEOF();

    abstract public boolean isSpaceAvailable(int len);

    abstract public boolean eof();

    abstract public int write(byte[] bytes) ;

    abstract public byte[] read();

    abstract public byte[] read(int readPos);

    abstract public int countRecord();

    abstract public void sync();

    abstract public void close();








}
