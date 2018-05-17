package com.yudy.heze.store.index;

import com.yudy.heze.store.pool.TopicQueuePool;
import com.yudy.heze.zk.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Cleaner;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.security.AccessController;
import java.security.PrivilegedAction;

public class DiskAndZkTopicQueueIndex implements TopicQueueIndex {
    private static final Logger LOGGER = LoggerFactory.getLogger(DiskAndZkTopicQueueIndex.class);

    private static final String INDEX_FILE_SUFFIX = ".umq";
    private volatile int readNum;        // 8   读索引文件号
    private volatile int readPosition;   // 12   读索引位置
    private volatile int readCounter;    // 16   总读取数量
    private volatile int writeNum;       // 20  写索引文件号
    private volatile int writePosition;  // 24  写索引位置
    private volatile int writeCounter;   // 28 总写入数量

    private RandomAccessFile indexFile;
    private FileChannel fileChannel;
    private MappedByteBuffer index;

    private ZkTopicQueueReadIndex zkIndex;
    private volatile int zkSYncTimes=0;

    public DiskAndZkTopicQueueIndex(String queueName, String fileDir, ZkClient zkClient) {
        String indexFilePath=formatIndexFilePath(queueName,fileDir);
        File file=new File(indexFilePath);
        try{
            if (file.exists()){
                this.indexFile=new RandomAccessFile(file,"rw");
                byte[] bytes=new byte[8];
                this.indexFile.read(bytes,0,8);
                if (!MAGIC.equals(new String(bytes))){
                    throw new IllegalArgumentException("version mismatch");
                }
                this.readNum = indexFile.readInt();
                this.readPosition = indexFile.readInt();
                this.readCounter = indexFile.readInt();
                this.writeNum = indexFile.readInt();
                this.writePosition = indexFile.readInt();
                this.writeCounter = indexFile.readInt();
                this.fileChannel = indexFile.getChannel();
                this.index=fileChannel.map(FileChannel.MapMode.READ_WRITE,0,INDEX_SIZE);
                this.index=index.load();
            }else{
                this.indexFile=new RandomAccessFile(file,"rw");
                this.fileChannel = indexFile.getChannel();
                this.index=fileChannel.map(FileChannel.MapMode.READ_WRITE,0,INDEX_SIZE);

                putMagic();
                putReadNum(0);
                putReadPosition(0);
                putReadCounter(0);
                putWriteNum(0);
                putWritePosition(0);
                putWriteCounter(0);
                this.index=index.load();
            }
        }catch (Exception e){
            throw new IllegalArgumentException(e);
        }
        if (null!=zkClient){
            this.zkIndex=new ZkTopicQueueReadIndex(queueName,zkClient);
        }
        if (zkIndex.getReadNum()>this.readNum){
            this.readNum=zkIndex.getReadNum();
        }
        if (zkIndex.getReadCounter()>this.readCounter){
            this.readCounter=zkIndex.getReadCounter();
        }
        if (zkIndex.getReadPosition()>this.readPosition){
            this.readPosition=zkIndex.getReadPosition();
        }

    }

    public static String formatIndexFilePath(String queueName,String fileDir){
        return fileDir+ File.separator+String.format("tindex_%s%s",queueName,INDEX_FILE_SUFFIX);
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


    @Override
    public void putMagic() {
        this.index.position(0);
        this.index.put(MAGIC.getBytes());
        if (zkIndex!=null)
            zkIndex.putMagic();
    }

    @Override
    public void putWritePosition(int writePosition) {
        this.index.position(WRITE_POS_OFFSET);
        this.index.putInt(writePosition);
        if (zkIndex!=null)
            zkIndex.putWritePosition(writePosition);
        this.writePosition=writePosition;
    }

    @Override
    public void putWriteNum(int writeNum) {
        this.index.position(WRITE_NUM_OFFSET);
        this.index.putInt(writeNum);
        if (zkIndex!=null)
            zkIndex.putWriteNum(writeNum);
        this.writeNum=writeNum;

    }

    @Override
    public void putWriteCounter(int writeCounter) {
        this.index.position(WRITE_CNT_OFFSET);
        this.index.putInt(writeCounter);
        if(zkIndex != null){
            zkIndex.putWriteCounter(writeCounter);
        }
        this.writeCounter = writeCounter;

    }

    @Override
    public void putReadNum(int readNum) {
        this.index.position(READ_NUM_OFFSET);
        this.index.putInt(readNum);
        if(zkIndex != null){
            zkIndex.putReadNum(readNum);
        }
        this.readNum = readNum;

    }

    @Override
    public void putReadPosition(int readPosition) {
        this.index.position(READ_POS_OFFSET);
        this.index.putInt(readPosition);
        if(zkIndex != null){
            zkIndex.putReadPosition(readPosition);
        }
        this.readPosition = readPosition;

    }

    @Override
    public void putReadCounter(int readCounter) {
        this.index.position(READ_CNT_OFFSET);
        this.index.putInt(readCounter);
        if(zkIndex != null){
            zkIndex.putReadCounter(readCounter);
        }
        this.readCounter = readCounter;
    }

    @Override
    public void reset() {
        int size = writeCounter - readCounter;
        putReadCounter(0);
        if(zkIndex != null){
            zkIndex.putReadCounter(0);
        }
        putWriteCounter(size);
        if (size == 0 && readNum == writeNum) {
            putReadPosition(0);
            putWritePosition(0);
            if(zkIndex != null){
                zkIndex.putReadPosition(0);
            }
        }

    }

    @Override
    public void sync() {
        if (index!=null){
            index.force();
            index.position(0);
            StringBuilder sb  = new StringBuilder();
            byte[] bytes = new byte[8];
            index.get(bytes, 0, 8);
            sb.append("disk index").append("=>").append("readNum:").append(index.getInt())
                    .append(",readPosition:").append(index.getInt())
                    .append(",readCounter:").append(index.getInt())
                    .append(",writeNum:").append(index.getInt())
                    .append(",writePosition:").append(index.getInt())
                    .append(",writeCounter:").append(index.getInt());

        }
        if (null!=this.zkIndex){
            if (zkSYncTimes>= TopicQueuePool.ZK_SYNC_DATA_PERSISTENCE_INTERVAL){
                try {
                    zkIndex.sync2zk(getReadNum(),getReadPosition(),getReadCounter(),getWriteNum(),getWritePosition(),getWriteCounter());
                }catch (Exception e){
                    LOGGER.error("sync to zookeeper error",e);
                }
                StringBuilder sb  = new StringBuilder();
                sb.append("zk index").append("=>").append("readNum:").append(getReadNum())
                        .append(",readPosition:").append(getReadPosition())
                        .append(",readCounter:").append(getReadCounter())
                        .append(",writeNum:").append(getWriteNum())
                        .append(",writePosition:").append(getWritePosition())
                        .append(",writeCounter:").append(getWriteCounter());
                zkSYncTimes = 0;

            }
        }
        zkSYncTimes++;
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
            zkIndex.close();
        } catch (IOException e) {
            LOGGER.error("close fqueue index file failed", e);
        }

    }

    public static void main(String[] args) throws IOException {
        String path="/Users/yangyudong/source.code.learn/Heze/README.md";
        RandomAccessFile indexFile=new RandomAccessFile(path,"rw");
        FileChannel fileChannel = indexFile.getChannel();
        MappedByteBuffer index=fileChannel.map(FileChannel.MapMode.READ_WRITE,0,32);
        index.clear();
        index.force();
        System.out.println(index.isLoaded());
    }
}
