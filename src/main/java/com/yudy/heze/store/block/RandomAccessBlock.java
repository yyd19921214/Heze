package com.yudy.heze.store.block;

import com.yudy.heze.store.index.TopicQueueIndex;
import com.yudy.heze.store.index.RandomAccessBlockIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Cleaner;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * 一种支持数据随机访问的文件结构
 * 利用Index完成对数据的定位
 * 具体实现见链接: https://tech.meituan.com/kafka-fs-design-theory.html
 */
public class RandomAccessBlock extends AbstractQueueBlock {

    private final Logger LOG = LoggerFactory.getLogger(RandomAccessBlock.class);

    public static final String BLOCK_FILE_SUFFIX = ".umq";//数据文件

    private static final String BLOCK_FILE_PREFIX = "block";

    public static int BLOCK_SIZE = 32 * 1024 * 1024;//32MB

    private String blockFilePath;
    private RandomAccessBlockIndex index;
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

    public RandomAccessBlockIndex getIndex(){
        return this.index;
    }

    public RandomAccessBlock duplicate() {
        //todo need to be refined
        return new RandomAccessBlock(this.blockFilePath, this.index, this.byteBuffer.duplicate(), this.mappedBlock, this.fileChannel, this.blockFile);
    }

    @Override
    public void putEOF() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isSpaceAvailable(int len) {
        int lastRecordPos=index.getLastRecordPosition();
        byteBuffer.getLong(lastRecordPos);
        int lastRecordLen=byteBuffer.getInt();
        int skipToPos=lastRecordPos+Long.BYTES+Integer.BYTES+lastRecordLen;
        int increment = len + Integer.BYTES +Long.BYTES;//len(offset)+len(len)+len(msg);
        return BLOCK_SIZE >= increment + skipToPos + 4;
    }

    @Override
    public boolean eof() {
        throw new UnsupportedOperationException();
    }

    /**
     * the structure of single message is like this
     * ----------------
     * offset(long)
     * len of message(int)
     * contents of message(bytes)
     * ----------------
     * @param bytes
     * @return
     */
//    @Override
//    public int write(byte[] bytes) {
//        int pos=index.getLastRecordPosition();
//        long offset=byteBuffer.getLong(pos);
//        int len=byteBuffer.getInt();
//        int skipToPos=pos+Long.BYTES+Integer.BYTES+len;
//        byteBuffer.position(skipToPos);
//        byteBuffer.putLong(offset+1);
//        byteBuffer.putInt(bytes.length);
//        byteBuffer.put(bytes);
//        index.updateIndex(offset+1,skipToPos);
//        return skipToPos;
//    }

    public long write(byte[] bytes) {
        //todo
        // write the message and return the offset of this message
        return 1L;
    }


    @Override
    public byte[] read(int offset) {
        int pos=this.index.getReadPosition(offset);
        byteBuffer.position(pos);
        byteBuffer.getLong();
        int len=byteBuffer.getInt();
        byte[] bytes=new byte[len];
        byteBuffer.get(bytes);
        return bytes;
    }

    public byte[] read(long offset) {
        //todo
        return null;
//        int pos=this.index.getReadPosition(offset);
//        byteBuffer.position(pos);
//        byteBuffer.getLong();
//        int len=byteBuffer.getInt();
//        byte[] bytes=new byte[len];
//        byteBuffer.get(bytes);
//        return bytes;
    }

    @Override
    public int countRecord() {
        return index.getTotalNum();
    }

    @Override
    public byte[] read() {
        throw new UnsupportedOperationException();
    }



    @Override
    public void sync() {
        if (mappedBlock != null)
            mappedBlock.force();

    }

    @Override
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


}
