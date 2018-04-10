package com.yudy.heze.server.backup;

import com.yudy.heze.store.disk.DiskTopicQueueIndex;
import com.yudy.heze.store.zk.ZkTopicQueueReadIndex;
import com.yudy.heze.zk.ZkClient;

import java.util.AbstractQueue;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class BackupQueue extends AbstractQueue<byte[]>{

    private String queueName;
    private String fileDir;
    private DiskTopicQueueIndex writeIndex;
    private ZkTopicQueueReadIndex readIndex;
//    private BackupQueueBlock readBlock;
//    private BackupQueueBlock writeBlock;
    private ReentrantLock readLock;
    private ReentrantLock writeLock;
    private AtomicInteger size;

    public DiskTopicQueueIndex getWriteIndex() {
        return writeIndex;
    }

    public ZkTopicQueueReadIndex getReadIndex() {
        return readIndex;
    }

    public BackupQueue(String queueName, String fileDir, ZkClient zkClient) {
        this.queueName = queueName;
        this.fileDir = fileDir;
        this.readLock = new ReentrantLock();
        this.writeLock = new ReentrantLock();

    }

    @Override
    public byte[] poll() {
        return new byte[0];
    }

    @Override
    public boolean offer(byte[] bytes) {
        return false;
    }

    @Override
    public Iterator<byte[]> iterator() {
        return null;
    }

    @Override
    public byte[] peek() {
        return new byte[0];
    }

    @Override
    public int size() {
        return 0;
    }
}
