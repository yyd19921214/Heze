package com.yudy.heze.store;

import com.yudy.heze.network.Message;
import com.yudy.heze.store.disk.DiskAndZkTopicQueueIndex;
import com.yudy.heze.store.zk.ZkTopicQueueReadIndex;
import com.yudy.heze.zk.ZkClient;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractQueue;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class TopicQueue extends AbstractQueue<byte[]> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TopicQueue.class);

    private String queueName;
    private String fileDir;
    private TopicQueueIndex index;
    private TopicQueueBlock readBlock;
    private TopicQueueBlock replicaBlock;
    private TopicQueueBlock writeBlock;
    private ReentrantLock readLock;
    private ReentrantLock writeLock;
    private AtomicInteger size;

    public TopicQueue(String queueName, String fileDir, boolean backup) {
        this(queueName, fileDir, null, backup);
    }

    public TopicQueue(String queueName, String fileDir, ZkClient zkClient, boolean backup) {
        this.queueName = queueName;
        this.fileDir = fileDir;
        if (backup && zkClient != null) {
            // 这是一个backuo队列
            this.index = new ZkTopicQueueReadIndex(queueName,zkClient);
        } else {
            this.index = new DiskAndZkTopicQueueIndex(queueName, fileDir, zkClient);

        }
        this.size = new AtomicInteger(index.getWriteCounter() - index.getReadCounter());
        this.writeBlock = new TopicQueueBlock(index, TopicQueueBlock.formatBlockFilePath(queueName, index.getWriteNum(), fileDir));
        if (index.getReadNum() == index.getWriteCounter()) {
            this.readBlock = this.writeBlock.duplicate();
        } else {
            this.readBlock = new TopicQueueBlock(index, TopicQueueBlock.formatBlockFilePath(queueName, index.getReadNum(), fileDir));
        }
    }

    public TopicQueueIndex getReadIndex() {
        return index;
    }

    @Override
    public Iterator<byte[]> iterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int size() {
        return this.size.get();
    }

    @Override
    public boolean offer(byte[] bytes) {
        if (ArrayUtils.isEmpty(bytes))
            return true;
        writeLock.lock();
        try{
            if (!writeBlock.isSpaceAvailable(bytes.length))
                rotateNextWriteBlock();
            writeBlock.write(bytes);
            size.incrementAndGet();
            return true;
        }finally {
            writeLock.unlock();
        }

    }

    @Override
    public byte[] poll() {
        readLock.lock();
        try{
            if (readBlock.eof()){
                rotateNextWriteBlock();
            }
            byte[] bytes=readBlock.read();
            if (bytes!=null)
                size.decrementAndGet();
            return bytes;
        }finally {
            readLock.unlock();
        }


    }

    @Override
    public byte[] peek() {
        throw new UnsupportedOperationException();
    }

    public boolean write(Message... messages){
        PooledByteBufAllocator pooledByteBufAllocator=new PooledByteBufAllocator();
        ByteBuf byteBuf=pooledByteBufAllocator.directBuffer();
        if (null!=messages){
            for (Message message :messages){
                message.writeToByteBuf(byteBuf);
            }
        }
        if (byteBuf.hasArray()){
            offer(byteBuf.array());
            return true;
        }
        return false;
    }

    public Message read(){
        byte[] bytes=poll();
        if (bytes!=null){
            PooledByteBufAllocator pooledByteBufAllocator = new PooledByteBufAllocator();
            ByteBuf byteBuf=pooledByteBufAllocator.directBuffer(bytes.length);
            byteBuf.writeBytes(bytes);
            return Message.buildFromByteBuf(byteBuf);
        }
        return null;
    }

    public byte[] replicaRead(int readNum,int readPosition){
        if (this.replicaBlock==null)
            this.replicaBlock=new TopicQueueBlock(null,TopicQueueBlock.formatBlockFilePath(queueName,readNum,fileDir));
        String fname=this.replicaBlock.getBlockFilePath();
        String[] names=fname.split("_");
        if (StringUtils.isNotBlank(names[2])){
            String numStr=names[2].substring(0,names[2].indexOf("."));
            if (!numStr.equals(String.valueOf(readNum))){
                this.replicaBlock.close();
                this.replicaBlock=new TopicQueueBlock(null,TopicQueueBlock.formatBlockFilePath(queueName,readNum,fileDir));
            }
        }
        readLock.lock();
        try{
            byte[] bytes=replicaBlock.read(readPosition);
            return bytes;
        }finally {
            readLock.unlock();
        }
    }

    private void rotateNextWriteBlock() {
        int nextWriteBlockNum = index.getWriteNum() + 1;
        nextWriteBlockNum = (nextWriteBlockNum < 0) ? 0 : nextWriteBlockNum;
        writeBlock.putEOF();
        if (index.getReadNum() == index.getWriteNum())
            writeBlock.sync();
        else
            writeBlock.close();
        this.writeBlock = new TopicQueueBlock(index, TopicQueueBlock.formatBlockFilePath(queueName, nextWriteBlockNum, fileDir));
        index.putWriteNum(nextWriteBlockNum);
        index.putWritePosition(0);
    }

    private void rotateNextReadBlock() {
        if (index.getReadNum()==index.getWriteNum())
            return;
        int nextReadBlockNum = index.getReadNum() + 1;
        nextReadBlockNum = (nextReadBlockNum < 0) ? 0 : nextReadBlockNum;
        readBlock.close();
        String blockPath=readBlock.getBlockFilePath();
        if (nextReadBlockNum==index.getWriteNum())
            this.readBlock=this.writeBlock.duplicate();
        else
            this.readBlock=new TopicQueueBlock(index,TopicQueueBlock.formatBlockFilePath(queueName,nextReadBlockNum,fileDir));
        index.putReadNum(nextReadBlockNum);
        index.putReadPosition(0);
        TopicQueuePool.toClear(blockPath);
    }

    public void sync(){
        try{
            index.sync();
        }catch (Exception e){
            LOGGER.error("unable to sync...");
        }
        writeBlock.sync();
    }

    public void close(){
        writeBlock.close();
        if (index.getReadNum()!=index.getWriteNum())
            readBlock.close();
        index.reset();
        index.close();
    }







}
