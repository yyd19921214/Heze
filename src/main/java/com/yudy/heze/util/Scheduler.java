package com.yudy.heze.util;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class Scheduler {

    private final Logger logger= LoggerFactory.getLogger(Scheduler.class);
    final AtomicLong threadId=new AtomicLong(0);
    final ScheduledThreadPoolExecutor executor;
    final String baseThreadName;

    public Scheduler(int numThreads, final String baseThreadName, final boolean isDaemon){
        this.baseThreadName=baseThreadName;
        executor=new ScheduledThreadPoolExecutor(numThreads, new ThreadFactory() {
            @Override
            public Thread newThread(@NotNull Runnable r) {
                Thread t=new Thread(r,baseThreadName+threadId.getAndIncrement());
                t.setDaemon(false);
                return t;
            }
        });
        executor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);

    }

    public ScheduledFuture<?> scheduleWithRate(Runnable command, long delayMs, long periodMs) {
        return executor.scheduleAtFixedRate(command, delayMs, periodMs, TimeUnit.MILLISECONDS);
    }

    public void shutdownNow() {
        executor.shutdownNow();
        logger.info("ShutdownNow scheduler {} with {} threads.", baseThreadName, threadId.get());
    }

    public void shutdown() {
        executor.shutdown();
        logger.info("Shutdown scheduler {} with {} threads.", baseThreadName, threadId.get());
    }



}
