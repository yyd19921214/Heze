package com.yudy.heze.zk;

import com.yudy.heze.exception.ZkNoNodeException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
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
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

public class ZkClient implements Watcher, Closeable {

    private static final int DEFAULT_CONNECTION_TIMEOUT = 10000;

    private static final int DEFAULT_SESSION_TIMEOUT = 30000;

    private static final Logger LOG = LoggerFactory.getLogger(ZkClient.class);

    protected ZkConnection zkConnection;

    private final Map<String, Set<ZkChildListener>> _childListener = new ConcurrentHashMap<>();

    private final Map<String, Set<ZkDataListener>> _dataListener = new ConcurrentHashMap<>();

    private final Set<ZkStateListener> _stateListener = new CopyOnWriteArraySet<>();

    private volatile KeeperState _currentState;

    private final ZkLock _zkEventLock = new ZkLock();

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


    public ZkClient(ZkConnection zkConnection, int connectionTimeout) {
        this.zkConnection = zkConnection;
        //TODO
    }

    public List<String> subscribeChildChanges(String path, ZkChildListener listener) {
        synchronized (_childListener) {
            Set<ZkChildListener> listeners = _childListener.get(path);
            if (listeners == null) {
                listeners = new CopyOnWriteArraySet<>();
                _childListener.put(path, listeners);
            }

            listeners.add(listener);
        }
        return watchForChilds(path);
    }

    public void unsubscribeChildChanges(String path, ZkChildListener listener) {
        synchronized (_childListener) {
            Set<ZkChildListener> listeners = _childListener.get(path);
            if (listeners != null) {
                listeners.remove(listener);
            }

        }
    }

    public void subscribeDataChanges(String path, ZkDataListener listener) {
        synchronized (_dataListener) {
            Set<ZkDataListener> listeners = _dataListener.get(path);
            if (listeners == null) {
                listeners = new CopyOnWriteArraySet<ZkDataListener>();
                _dataListener.put(path, listeners);
            }
            listeners.add(listener);
        }

        watchForChilds(path);
        LOG.debug("Subscribed data changes for " + path);
    }

    public void unsubscribeDataChanges(String path, ZkDataListener listener) {
        synchronized (_dataListener) {
            Set<ZkDataListener> listeners = _dataListener.get(path);
            if (listeners != null)
                listeners.remove(listener);

        }
    }

    public void subscribeStateChanges(final ZkStateListener listener) {
        _stateListener.add(listener);
    }

    public void unsubscribeStateChanges(ZkStateListener listener) {
        _stateListener.remove(listener);
    }

    public void unsubscribeAll() {
        _childListener.clear();
        _dataListener.clear();
        _stateListener.clear();
    }


    public String create(final String path, byte[] data, final CreateMode mode) {
        if (path == null || path.length() == 0)
            throw new NullPointerException("path can not be null");
        final byte[] bytes = data;

        return retryUntilConnected(() -> zkConnection.create(path, data, mode));
    }

    public void createEphemeral(final String path, final byte[] data) {
        create(path, data, CreateMode.EPHEMERAL);
    }

    public void createEphemeralSequential(String path, byte[] data) {
        create(path, data, CreateMode.EPHEMERAL_SEQUENTIAL);
    }

    public ZkLock getEventLock() {
        return _zkEventLock;
    }


    public <E> E retryUntilConnected(Callable<E> callable) {
        while (true) {
            try {
                return callable.call();
            } catch (KeeperException.ConnectionLossException e) {
                Thread.yield();
//                waitUntilConnected();
            } catch (KeeperException.SessionExpiredException e) {
                Thread.yield();
//                waitUntilConnected();
            } catch (KeeperException e) {
//                throw ZkE
            } catch (InterruptedException e) {

            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }


    public List<String> watchForChilds(final String path) {
        return retryUntilConnected(
                () -> {
                    exists(path, true);
                    try {
                        return getChildren(path, true);
                    } catch (ZkNoNodeException e) {

                    }
                    return null;
                }
        );
    }

    protected boolean exists(final String path, final boolean watch) {
        return retryUntilConnected(() -> zkConnection.exists(path, true));
    }

    protected List<String> getChildren(final String path, final boolean watch) {
        try {
            return retryUntilConnected(() -> zkConnection.getChildren(path, watch));
        } catch (ZkNoNodeException e) {
            return null;
        }

    }

    public boolean getShutdownTrigger() {
        return _shutdownTriggered;
    }


    @Override
    public void process(WatchedEvent watchedEvent) {
        LOG.debug("Received event: " + watchedEvent);
        _zooKeeperEventThread = Thread.currentThread();

        boolean stateChanged = watchedEvent.getPath() == null;
        boolean znodeChanged = watchedEvent.getPath() != null;
        boolean dataChanged =
                watchedEvent.getType() == Event.EventType.NodeDeleted || watchedEvent.getType() == Event.EventType.NodeChildrenChanged
                        || watchedEvent.getType() == Event.EventType.NodeCreated || watchedEvent.getType() == Event.EventType.NodeDataChanged;

        getEventLock().lock();
        try {
            if (getShutdownTrigger()) {
                LOG.debug("ignoring event '{" + watchedEvent.getType() + " | " + watchedEvent.getPath() + "}' since shutdown triggered");

            }
            if (stateChanged) {
                //TODO
            }
            if (dataChanged) {
                //TODO
            }

        } finally {
            if (stateChanged) {
                getEventLock().getStateChangedCondition().signalAll();
                if (watchedEvent.getState() == KeeperState.Expired) {
                    getEventLock().getZNodeEventCondition().signalAll();
                    getEventLock().getDataChangedCondition().signalAll();
                    //TODO
                }
            }
            if (znodeChanged){
                getEventLock().getZNodeEventCondition().signalAll();
            }
            if (dataChanged){
                getEventLock().getDataChangedCondition().signalAll();
            }
            getEventLock().unlock();
            LOG.debug("Leaving process event");
        }

    }

    @Override
    public void close() throws IOException {

    }
}
