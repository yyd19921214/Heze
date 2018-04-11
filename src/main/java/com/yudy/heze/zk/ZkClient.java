package com.yudy.heze.zk;

import com.yudy.heze.exception.*;
import org.apache.zookeeper.*;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;

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
        connect(connectionTimeout, this);
        System.out.println("connected.....");
    }



    public void createPersistent(String path, boolean createParents) {
        try {
            create(path, null, CreateMode.PERSISTENT);

        } catch (ZkNodeExistsException e) {
            if (!createParents)
                throw e;
        } catch (ZkNoNodeException e) {
            if (!createParents) {
                throw e;
            }
            String parentDir = path.substring(0, path.lastIndexOf("/"));
            createPersistent(parentDir, createParents);
            createPersistent(path, createParents);
        }
    }



    public String create(final String path, byte[] data, final CreateMode mode) {
        if (path == null || path.length() == 0)
            throw new NullPointerException("path can not be null");
        final byte[] bytes = data;

        return retryUntilConnected(() -> zkConnection.create(path, data, mode));
    }

    public void createEphemeral(String path, byte[] data) {
        create(path, data, CreateMode.EPHEMERAL);
    }

    public String createEphemeralSequential(String path, byte[] data) {
        return create(path, data, CreateMode.EPHEMERAL_SEQUENTIAL);
    }

    public ZkLock getEventLock() {
        return _zkEventLock;
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
                processStateChanged(watchedEvent);
            }
            if (dataChanged) {
                processDataOrChildChange(watchedEvent);
            }

        } finally {
            if (stateChanged) {
                getEventLock().getStateChangedCondition().signalAll();
                if (watchedEvent.getState() == KeeperState.Expired) {
                    getEventLock().getZNodeEventCondition().signalAll();
                    getEventLock().getDataChangedCondition().signalAll();
                    fireAllEvents();
                }
            }
            if (znodeChanged) {
                getEventLock().getZNodeEventCondition().signalAll();
            }
            if (dataChanged) {
                getEventLock().getDataChangedCondition().signalAll();
            }
            getEventLock().unlock();
            LOG.debug("Leaving process event");
        }

    }

    private void fireAllEvents() {
        for (String path : _childListener.keySet()) {
            fireChildChangeEvents(path, _childListener.get(path));
        }
        for (String path : _dataListener.keySet()) {
            fireDataChangedEvents(path, _dataListener.get(path));
        }
    }


    public List<String> getChildren(final String path) {
        return getChildren(path, hasListener(path));
    }

    protected List<String> getChildren(final String path, final boolean watch) {
        try {
            return retryUntilConnected(() -> zkConnection.getChildren(path, watch));
        } catch (ZkNoNodeException e) {
            return null;
        }
    }



    public boolean exists(final String path, final boolean watch) {
        return retryUntilConnected(() -> zkConnection.exists(path, true));
    }

    public boolean exists(final String path) {
        return exists(path, hasListener(path));
    }

    private void processStateChanged(WatchedEvent watchedEvent) {
        LOG.info("zookeeper state changed (" + watchedEvent.getState() + ")");
        setCurrentState(watchedEvent.getState());
        if (getShutdownTrigger())
            return;
        try {
            fireStateChangedEvents(watchedEvent.getState());
            if (watchedEvent.getState() == KeeperState.Expired) {
                reconnect();
                fireNewSessionEvents();
            }


        } catch (Exception e) {
            throw new RuntimeException("Exception while restarting zk client", e);
        }

    }

    private void fireNewSessionEvents() {
        for (ZkStateListener stateListener : _stateListener) {
            _eventThread.send(new ZkEventThread.ZkEvent(" new session start and listener by" + stateListener) {
                @Override
                public void run() throws Exception {
                    stateListener.handleNewSession();
                }
            });

        }
    }

    private void fireStateChangedEvents(final KeeperState state) {
        for (ZkStateListener stateListener : _stateListener) {
            _eventThread.send(new ZkEventThread.ZkEvent("State changed to " + state + "listener by" + stateListener) {
                @Override
                public void run() throws Exception {
                    stateListener.handleStateChange(state);
                }
            });
        }
    }


    private boolean hasListener(String path) {
        Set<ZkDataListener> dataListeners = _dataListener.get(path);
        if (dataListeners != null && dataListeners.size() > 0) {
            return true;
        }
        Set<ZkChildListener> childListeners = _childListener.get(path);
        if (childListeners != null && childListeners.size() > 0) {
            return true;
        }
        return false;
    }


    public boolean delete(final String path) {
        return retryUntilConnected(() -> {
            zkConnection.delete(path);
            return null;
        });
    }

    private void processDataOrChildChange(WatchedEvent watchedEvent) {
        String path = watchedEvent.getPath();
        if (watchedEvent.getType() == EventType.NodeCreated || watchedEvent.getType() == EventType.NodeChildrenChanged
                || watchedEvent.getType() == EventType.NodeDeleted) {
            Set<ZkChildListener> childListeners = _childListener.get(path);
            if (childListeners != null && !childListeners.isEmpty()) {
               fireChildChangeEvents(path,childListeners);
            }
        }

        if (watchedEvent.getType() == EventType.NodeDataChanged || watchedEvent.getType() == EventType.NodeDeleted
                || watchedEvent.getType() == EventType.NodeCreated) {
            Set<ZkDataListener> dataListeners = _dataListener.get(path);
            if (dataListeners != null && !dataListeners.isEmpty()) {
                fireDataChangedEvents(watchedEvent.getPath(), dataListeners);
            }
        }
    }


    private void fireDataChangedEvents(String path, Set<ZkDataListener> listeners) {
        for (ZkDataListener listener : listeners) {
            _eventThread.send(new ZkEventThread.ZkEvent("data of " + path + " changed sent to " + listener) {
                @Override
                public void run() throws Exception {
                    exists(path, true);
                    try {
                        byte[] data = readData(path, null, true);
                        listener.handleDataChange(path, data);
                    } catch (ZkNoNodeException e) {
                        listener.handleDataDeleted(path);
                    }
                }
            });
        }
    }

    private void fireChildChangeEvents(String path, Set<ZkChildListener> listeners) {
        for (ZkChildListener listener : listeners) {
            _eventThread.send(new ZkEventThread.ZkEvent("CHILD of " + path + " changed listenered by " + listener) {
                @Override
                public void run() throws Exception {

                    try {
                        exists(path);
                        List<String> children = getChildren(path, true);
                        listener.handleChildChange(path, children);
                    } catch (ZkNoNodeException e) {
                        listener.handleChildChange(path, null);
                    }
                }
            });
        }
    }



    public boolean waitUntilConnected() throws ZkInterruptedException {
        return waitUntilConnected(Integer.MAX_VALUE, TimeUnit.MILLISECONDS);
    }

    public boolean waitUntilConnected(long waitTime, TimeUnit timeUnit) {
        return waitForKeeperState(KeeperState.SyncConnected, waitTime, timeUnit);

    }

    private boolean waitForKeeperState(KeeperState keeperState, long time, TimeUnit timeUnit) {
        if (_zooKeeperEventThread != null && Thread.currentThread() == _zooKeeperEventThread) {
            throw new IllegalArgumentException("can not be done in zookeeper event thread");
        }
        Date timeWait = new Date(System.currentTimeMillis() + timeUnit.toMillis(time));
        try {
            getEventLock().lockInterruptibly();
            boolean waiting = true;
            while (_currentState != keeperState) {
                if (!waiting)
                    return false;
                waiting = getEventLock().getStateChangedCondition().awaitUntil(timeWait);
            }
            LOG.debug("State is " + _currentState);
            return true;
        } catch (InterruptedException e) {
            throw new ZkInterruptedException(e);
        } finally {
            getEventLock().unlock();
        }
    }


    private void acquireEventLock() {
        try {
            getEventLock().lockInterruptibly();
        } catch (InterruptedException e) {
            throw new ZkInterruptedException(e);
        }
    }


    public <E> E retryUntilConnected(Callable<E> callable) {
        if (_zooKeeperEventThread != null && Thread.currentThread() == _zooKeeperEventThread) {
            throw new IllegalArgumentException("Must not be done in the zookeeper event thread.");
        }
        while (true) {
            try {
                return callable.call();
            } catch (KeeperException.ConnectionLossException e) {
                e.printStackTrace();
                Thread.yield();
                waitUntilConnected();
            } catch (KeeperException.SessionExpiredException e) {
                e.printStackTrace();
                Thread.yield();
                waitUntilConnected();
            } catch (KeeperException e) {
                throw ZkException.create(e);
            } catch (InterruptedException e) {
                throw new ZkInterruptedException(e);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    public void setCurrentState(KeeperState currentState) {
        getEventLock().lock();
        try {
            _currentState = currentState;
        } finally {
            getEventLock().unlock();
        }
    }


    public byte[] readData(String path) {
        return readData(path, false);
    }


    public byte[] readData(String path, boolean returnNullIfPathNotExists) {
        byte[] data = null;
        try {
            data = readData(path, null);
        } catch (ZkNoNodeException e) {
            if (!returnNullIfPathNotExists)
                throw e;
        }
        return data;

    }

    public byte[] readData(String path, Stat stat) {
        return readData(path, stat, hasListener(path));
    }

    protected byte[] readData(String path, Stat stat, boolean watch) {
        byte[] data = retryUntilConnected(() ->
                zkConnection.readData(path, stat, watch)
        );
        return data;
    }

    public Stat writeData(String path, byte[] object) {
        return writeData(path, object, -1);
    }


    public Stat writeData(final String path, final byte[] data, final int expectedVersion) {
        return retryUntilConnected(() -> zkConnection.writeData(path, data, -1));
    }




    public synchronized void connect(final long maxMsToWaitUntilConnected, Watcher watcher) {
        if (_eventThread != null)
            return;
        boolean started = false;
        try {
            getEventLock().lockInterruptibly();
            setShutDownTrigger(false);
            _eventThread = new ZkEventThread(zkConnection.getServers());
            _eventThread.start();
            zkConnection.connect(watcher);
            LOG.debug("Awaiting connection to Zookeeper server: " + maxMsToWaitUntilConnected);
            if (!waitUntilConnected(maxMsToWaitUntilConnected, TimeUnit.MILLISECONDS))
                LOG.error(String.format("Unable to connect to zookeeper server[%s] within timeout %dms", zkConnection.getServers(), maxMsToWaitUntilConnected));
            started = true;
        } catch (InterruptedException e) {
            ZooKeeper.States states = zkConnection.getZookeeperState();
            LOG.warn("unable to connect to server. current state is " + states);
        } finally {
            getEventLock().unlock();
            if (!started)
                close();

        }
    }



    public boolean getShutdownTrigger() {
        return _shutdownTriggered;
    }


    private void reconnect() throws ZkInterruptedException {
        getEventLock().lock();
        try {
            zkConnection.close();
            zkConnection.connect(this);
        } catch (InterruptedException e) {
            throw new ZkInterruptedException(e);
        } finally {
            getEventLock().unlock();
        }
    }


    private void setShutDownTrigger(boolean shutdownTriggered) {
        _shutdownTriggered = shutdownTriggered;
    }


    public synchronized void close() throws ZkInterruptedException {
        if (_eventThread == null) {
            return;
        }
        LOG.debug("Closing ZkClient...");
        getEventLock().lock();
        try {
            setShutDownTrigger(true);
            _currentState = null;
            _eventThread.interrupt();
            _eventThread.join(2000);
            zkConnection.close();
            _eventThread = null;
        } catch (InterruptedException e) {
            throw new ZkInterruptedException(e);
        } finally {
            getEventLock().unlock();
        }
        LOG.debug("Closing ZkClient...done");
    }

    public int numberOfListeners(){
        int listeners=0;
        for (Set<ZkChildListener> set:_childListener.values()){
            listeners+=set.size();
        }
        for (Set<ZkDataListener> set:_dataListener.values()){
            listeners+=set.size();
        }
        listeners+=_stateListener.size();
        return listeners;
    }

    public ZooKeeper getZooKeeper(){
        return zkConnection==null?null:zkConnection.getZookeeper();
    }

    public boolean isConnected(){
        return _currentState==KeeperState.SyncConnected;
    }

    interface DataUpdater {

        /**
         * Updates the current data of a znode.
         *
         * @param currentData The current contents.
         * @return the new data that should be written back to ZooKeeper.
         */
        public byte[] update(byte[] currentData);

    }


}
