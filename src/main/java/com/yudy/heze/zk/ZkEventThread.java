package com.yudy.heze.zk;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class ZkEventThread extends Thread {

    private static final Logger LOG = LoggerFactory.getLogger(ZkEventThread.class);

    private final BlockingQueue<ZkEvent> _events = new LinkedBlockingQueue<>();

    private static final AtomicInteger _eventId = new AtomicInteger(0);

    private volatile boolean shutdown = false;


    static abstract class ZkEvent {
        private final String _description;

        public ZkEvent(String _description) {
            this._description = _description;
        }

        public abstract void run() throws Exception;

        @Override
        public String toString() {
            return "ZkEvent{" +
                    "_description='" + _description + '\'' +
                    '}';
        }
    }

    public ZkEventThread(String name) {
        setDaemon(true);
        setName("ZkClient-EventThread-" + getId() + "-" + name);
    }

    public boolean isShutdown() {
        return shutdown || isInterrupted();
    }

    public void shutdown(){
        this.shutdown=true;
        this.interrupt();
    }

    public void send(ZkEvent event){
        if (!isShutdown()){
            LOG.debug("New event: "+event);
            _events.add(event);
        }
    }

    @Override
    public void run() {
        LOG.info("Starting ZkClient event thread");
        try {
            while (!isShutdown()){
                ZkEvent event=_events.take();
                int eventId=_eventId.getAndIncrement();
                LOG.debug("Delivering event #" + eventId + " " + event);

                try {
                    event.run();
                }catch (InterruptedException e) {
                    shutdown();
                } catch (Throwable e) {
                    LOG.error("Error handling event " + event, e);
                }
                LOG.debug("Delivering event #" + eventId + " " + event);
            }
        }catch (InterruptedException e){

        }
    }
}
