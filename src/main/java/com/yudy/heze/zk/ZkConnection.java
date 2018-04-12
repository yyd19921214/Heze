package com.yudy.heze.zk;

import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

public class ZkConnection {

    private static final Logger LOG = LoggerFactory.getLogger(ZkConnection.class);

    private ZooKeeper _zk = null;

    private final Lock _zookeeperLock = new ReentrantLock();

    private final String _servers;

    private final int _sessionTimeOut;

    private final String _authStr;

    private final List<ACL> acls = new ArrayList<>();


    public ZkConnection(String zkServers, int sessionTimeOut, String authStr) {
        _servers = zkServers;
        _sessionTimeOut = sessionTimeOut;
        _authStr = authStr;
    }

    public void connect(Watcher watcher) {
        _zookeeperLock.lock();
        try {
            if (_zk != null)
                throw new IllegalStateException("zk client has already been started");
            LOG.debug("Creating new ZookKeeper instance to connect to " + _servers + ".");
            _zk = new ZooKeeper(_servers, _sessionTimeOut, watcher);
            if (StringUtils.isNotBlank(_authStr)) {
                acls.clear();
                acls.add(new ACL(ZooDefs.Perms.ALL, new Id("digest", DigestAuthenticationProvider.generateDigest(_authStr))));
                acls.add(new ACL(ZooDefs.Perms.READ, ZooDefs.Ids.ANYONE_ID_UNSAFE));
            }
        } catch (IOException | NoSuchAlgorithmException e) {
            e.printStackTrace();
        } finally {
            _zookeeperLock.unlock();
        }
    }

    public void close() throws InterruptedException {
        _zookeeperLock.lock();
        try {
            if (_zk != null) {
                LOG.debug("Closing Zookeeper connected to " + _servers);
                _zk.close();
                _zk = null;
            }
        } finally {
            _zookeeperLock.unlock();
        }
    }

    public String create(String path, byte[] data, CreateMode mode) throws KeeperException, InterruptedException,KeeperException.NoNodeException {
        return _zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, mode);
    }

    public void delete(String path) throws KeeperException, InterruptedException {
        _zk.delete(path, -1);
    }

    public boolean exists(String path, boolean watch) throws KeeperException, InterruptedException {
        return null != _zk.exists(path, watch);
    }

    public List<String> getChildren(final String path, final boolean watch) throws KeeperException, InterruptedException {
        return _zk.getChildren(path, watch);
    }

    public byte[] readData(String path, Stat stat, boolean watch) throws KeeperException, InterruptedException {
        return _zk.getData(path, watch, stat);
    }

    public Stat writeData(String path, byte[] data, int version) throws KeeperException, InterruptedException {
        return _zk.setData(path, data, version);
    }

    public ZooKeeper.States getZookeeperState() {
        return _zk != null ? _zk.getState() : null;
    }


    public String getServers() {
        return _servers;
    }

    public ZooKeeper getZookeeper() {
        return _zk;
    }

}
