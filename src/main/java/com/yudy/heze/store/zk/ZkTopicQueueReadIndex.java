package com.yudy.heze.store.zk;

import com.yudy.heze.store.TopicQueueIndex;

public class ZkTopicQueueReadIndex implements TopicQueueIndex {

    @Override
    public int getReadNum() {
        return 0;
    }

    @Override
    public int getReadPosition() {
        return 0;
    }

    @Override
    public int getReadCounter() {
        return 0;
    }

    @Override
    public int getWriteNum() {
        return 0;
    }

    @Override
    public int getWritePosition() {
        return 0;
    }

    @Override
    public int getWriteCounter() {
        return 0;
    }

    @Override
    public void putMagic() {

    }

    @Override
    public void putWritePosition(int writePosition) {

    }

    @Override
    public void putWriteNum(int writeNum) {

    }

    @Override
    public void putWriteCounter(int writeCounter) {

    }

    @Override
    public void putReadNum(int readNum) {

    }

    @Override
    public void putReadPosition(int readPosition) {

    }

    @Override
    public void putReadCounter(int readCounter) {

    }

    @Override
    public void reset() {

    }

    @Override
    public void sync() {

    }

    @Override
    public void close() {

    }
}
