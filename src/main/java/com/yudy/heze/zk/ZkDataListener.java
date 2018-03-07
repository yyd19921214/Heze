package com.yudy.heze.zk;

public interface ZkDataListener {

    public void handleDataChange(String dataPath, byte[] data) throws Exception;

    public void handleDataDeleted(String dataPath) throws Exception;
}
