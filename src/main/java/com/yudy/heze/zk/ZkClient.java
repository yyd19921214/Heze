package com.yudy.heze.zk;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.omg.CORBA.PRIVATE_MEMBER;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

public class ZkClient implements Watcher,Closeable{

    private static final int DEFAULT_CONNECTION_TIMEOUT = 10000;

    private static final int DEFAULT_SESSION_TIMEOUT = 30000;

    private static final Logger LOG= LoggerFactory.getLogger(ZkClient.class);

    protected ZkConnection zkConnection;

    private final Map<String,Set<ZkChildListener>> _childListener=new ConcurrentHashMap<>();

    private final Map<String,Set<ZkDataListener>> _dataListener=new ConcurrentHashMap<>();

    private final Set<ZkStateListener> _stateListener=new CopyOnWriteArraySet<>();

    private volatile KeeperState _currentState;

    private final ZkLock _zkEventLock=new ZkLock();

    private volatile boolean _shutdownTriggered;

    private ZkEventThread _eventThread;

    private Thread _zooKeeperEventThread;


    public ZkClient(String connectString, String authStr) {
        this(connectString, authStr, DEFAULT_CONNECTION_TIMEOUT);
    }

    public ZkClient(String connectString, String authStr, int connectionTimeout) {
        this(connectString, authStr, DEFAULT_SESSION_TIMEOUT, connectionTimeout);
    }

    public ZkClient(String connectString, String authStr, int sessionTimeout, int connectionTimeout) {
        this(new ZkConnection(connectString, sessionTimeout, authStr), connectionTimeout);
    }


    public ZkClient(ZkConnection zkConnection,int connectionTimeout){
        this.zkConnection=zkConnection;
        //TODO
    }

    public List<String> subscribeChildChanges(String path, ZkChildListener listener){
        synchronized (_childListener){
            Set<ZkChildListener> listeners=_childListener.get(path);
            if (listeners==null){
                listeners=new CopyOnWriteArraySet<>();
                _childListener.put(path,listeners);
            }

            listeners.add(listener);
        }

        return watchForChilds(path);

    }













    @Override
    public void process(WatchedEvent watchedEvent) {

    }

    @Override
    public void close() throws IOException {

    }
}
