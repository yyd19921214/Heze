package com.yudy.heze.store;

import com.yudy.heze.store.index.BasicTopicQueueIndex;
import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractQueue;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class BasicTopicQueue extends AbstractQueue<byte[]> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BasicTopicQueueBlock.class);

    private String queueName;
    private String fileDir;
    public TopicQueueIndex index;  // 该index用来做写index的同时也用来做初始化的读取index
    private BasicTopicQueueBlock readBlock;
    private BasicTopicQueueBlock writeBlock;
    private ReentrantLock readLock;
    private ReentrantLock writeLock;
    private AtomicInteger size;

//    /**
//     * 消息队列支持多个消费者组之间的隔离读取
//     * 即消费者组A读取数据或并不会影响消费者组B读取数据
//     * 为此我们需要维护一个映射保存不同消费者组的index文件
//     */
//
//    // 记录每个ConsumerGroup对应的读取块
//    private Map<String, BasicTopicQueueBlock> activeReadBlockMap;
//    // 记录每个ConsumerGroup对应的索引文件
//    private Map<String, BasicTopicQueueIndex> activeReadIndexMap;


    public BasicTopicQueue(String queueName, String fileDir) {
        this.queueName = queueName;
        this.fileDir = fileDir;
        this.readLock = new ReentrantLock();
        this.writeLock = new ReentrantLock();
        this.index = new BasicTopicQueueIndex(queueName, fileDir);
        this.size = new AtomicInteger(index.getWriteCounter());
        this.writeBlock = new BasicTopicQueueBlock(index, BasicTopicQueueBlock.formatBlockFilePath(queueName, index.getWriteNum(), fileDir));
//        this.activeReadBlockMap = new ConcurrentHashMap<>();
//        this.activeReadIndexMap = new ConcurrentHashMap<>();

        if (index.getReadNum() == index.getWriteNum()) {
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
        try {
            if (!writeBlock.isSpaceAvailable(bytes.length))
                rotateNextWriteBlock();
            writeBlock.write(bytes);
            size.incrementAndGet();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        } finally {
            writeLock.unlock();
        }

    }

    @Override
    public byte[] poll() {
        readLock.lock();
        try {
            if (readBlock.eof()) {
                rotateNextReadBlock();
            }
            byte[] bytes = readBlock.read();
            if (bytes != null)
                size.decrementAndGet();
            return bytes;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public byte[] peek() {
        throw new UnsupportedOperationException();
    }


    /**
     * 索引移动到消息队列的第一个block并从头开始读取
     *
     * @return
     */

    public boolean resetHead() {
        readLock.lock();
        try {
            if (index.getReadNum() != 0) {
                if (index.getWriteNum() != index.getReadNum()) {
                    this.readBlock.close();
                }
                index.putReadNum(0);
                if (index.getWriteNum() == 0) {
                    this.readBlock = this.writeBlock.duplicate();
                } else {
                    this.readBlock = new BasicTopicQueueBlock(index, BasicTopicQueueBlock.formatBlockFilePath(queueName, index.getReadNum(), fileDir));
                }
            }
            index.putReadPosition(0);
            index.putReadCounter(0);
            return true;
        } finally {
            readLock.unlock();
        }
    }

    /**
     * 定位到第{counter}条消息
     *
     * @param counter
     * @return
     */
    public boolean locate(int counter) {
        if (counter > index.getWriteCounter()) {
            throw new IllegalArgumentException();
        }
        int writeNum = index.getWriteNum();
        int readNum = index.getReadNum();
        BasicTopicQueueBlock _block = readBlock;
        int totalCnt = 0;
        int i = 0;
        for (; i <= writeNum; i++) {
            index.putReadNum(i);
            index.putReadPosition(0);

            if (i == readNum) {
                this.readBlock = _block;
            } else if (i == writeNum) {
                this.readBlock = this.writeBlock.duplicate();
            } else {
                this.readBlock = new BasicTopicQueueBlock(index, BasicTopicQueueBlock.formatBlockFilePath(queueName, i, fileDir));
            }
            int recordCnt = this.readBlock.countRecord();
            if (totalCnt + recordCnt >= counter) {
                for (int j = 1; j < counter - totalCnt; j++) {
                    this.readBlock.read();
                }
                break;
            } else {
                totalCnt += recordCnt;
                if (i != writeNum) {
                    this.readBlock.close();
                }
            }
        }
        index.putReadCounter(counter - 1);
        if (i != readNum) {

            _block.close();
        }
        return true;
    }

    public boolean skip(int step) {
        if (step >= 0) {
            return locate(this.index.getReadCounter() + step + 1);
        } else {
            if (this.index.getReadCounter() + step < 0) {
                throw new IllegalArgumentException();
            }
            return locate(this.index.getReadCounter() + step + 1);
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
        if (index.getReadNum() == index.getWriteNum())
            return;
        int nextReadBlockNum = index.getReadNum() + 1;
        nextReadBlockNum = (nextReadBlockNum < 0) ? 0 : nextReadBlockNum;
        readBlock.close();
        if (nextReadBlockNum == index.getWriteNum())
            this.readBlock = this.writeBlock.duplicate();
        else
            this.readBlock = new BasicTopicQueueBlock(index, BasicTopicQueueBlock.formatBlockFilePath(queueName, nextReadBlockNum, fileDir));
        index.putReadNum(nextReadBlockNum);
        index.putReadPosition(0);
    }

    public void sync() {
        try {
            index.sync();
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.error("unable to sync...");
        }
        writeBlock.sync();
    }

    public void close() {
        writeBlock.close();
        if (index.getReadNum() != index.getWriteNum())
            readBlock.close();
        index.close();
    }


}
