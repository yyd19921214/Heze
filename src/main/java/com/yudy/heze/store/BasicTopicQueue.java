package com.yudy.heze.store;

import com.yudy.heze.store.index.BasicTopicQueueIndex;
import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractQueue;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class BasicTopicQueue extends AbstractQueue<byte[]> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BasicTopicQueueBlock.class);

    private String queueName;
    private String fileDir;
    private TopicQueueIndex index;
    private BasicTopicQueueBlock readBlock;
    private BasicTopicQueueBlock writeBlock;
    private ReentrantLock readLock;
    private ReentrantLock writeLock;
    private AtomicInteger size;

    public BasicTopicQueue(String queueName, String fileDir) {
        this.queueName = queueName;
        this.fileDir = fileDir;
        this.readLock=new ReentrantLock();
        this.writeLock=new ReentrantLock();
        this.index=new BasicTopicQueueIndex(queueName,fileDir);
        this.size = new AtomicInteger(index.getWriteCounter() - index.getReadCounter());
        this.writeBlock = new BasicTopicQueueBlock(index, BasicTopicQueueBlock.formatBlockFilePath(queueName, index.getWriteNum(), fileDir));
        if (index.getReadNum() == index.getWriteCounter()) {
            this.readBlock = this.writeBlock.duplicate();
        } else {
            this.readBlock = new BasicTopicQueueBlock(index, BasicTopicQueueBlock.formatBlockFilePath(queueName, index.getReadNum(), fileDir));
        }
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
        }catch (Exception e){
            e.printStackTrace();
            return false;
        }
        finally {
            writeLock.unlock();
        }

    }

    @Override
    public byte[] poll() {
        readLock.lock();
        try{
            if (readBlock.eof()){
                rotateNextReadBlock();
            }
            byte[] bytes=readBlock.read();
            if (bytes!=null)
                size.decrementAndGet();
            return bytes;
        }catch (Exception e){
            e.printStackTrace();
            return null;
        }
        finally {
            readLock.unlock();
        }
    }

    @Override
    public byte[] peek() {
        throw new UnsupportedOperationException();
    }


    /**
     * 索引移动到消息队列的第一个block并从头开始读取
     * @return
     */

    public boolean resetHead(){
        readLock.lock();
        try {
            if (index.getReadNum()!=0){
                index.putReadNum(0);
                if (index.getWriteNum()==0){
                    this.readBlock=this.writeBlock.duplicate();
                }
                else{
                    this.readBlock = new BasicTopicQueueBlock(index, BasicTopicQueueBlock.formatBlockFilePath(queueName, index.getReadNum(), fileDir));
                }
            }
            index.putReadPosition(0);
            index.putReadCounter(0);
            return true;
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
        this.writeBlock = new BasicTopicQueueBlock(index, BasicTopicQueueBlock.formatBlockFilePath(queueName, nextWriteBlockNum, fileDir));
        index.putWriteNum(nextWriteBlockNum);
        index.putWritePosition(0);
    }

    private void rotateNextReadBlock() {
        if (index.getReadNum()==index.getWriteNum())
            return;
        int nextReadBlockNum = index.getReadNum() + 1;
        nextReadBlockNum = (nextReadBlockNum < 0) ? 0 : nextReadBlockNum;
        readBlock.close();
        if (nextReadBlockNum==index.getWriteNum())
            this.readBlock=this.writeBlock.duplicate();
        else
            this.readBlock=new BasicTopicQueueBlock(index,BasicTopicQueueBlock.formatBlockFilePath(queueName,nextReadBlockNum,fileDir));
        index.putReadNum(nextReadBlockNum);
        index.putReadPosition(0);
    }



}
