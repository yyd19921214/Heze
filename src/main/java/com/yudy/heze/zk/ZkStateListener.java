package com.yudy.heze.zk;

import org.apache.zookeeper.Watcher.Event.KeeperState;

public interface ZkStateListener {

    public void handleStateChange(KeeperState state) throws Exception;

    public void handleNewSession()throws Exception;

}
