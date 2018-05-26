package com.yudy.heze.store.pool;

import com.yudy.heze.config.ServerConfig;
import com.yudy.heze.store.queue.RandomAccessTopicQueue;
import com.yudy.heze.util.Scheduler;
import com.yudy.heze.util.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class RandomAccessQueuePool {
    private static final Logger LOGGER = LoggerFactory.getLogger(RandomAccessQueuePool.class);


    private static final String FILE_SUFFIX = ".umq";

    public static final String DEFAULT_DATA_PATH = "./data";

    private static RandomAccessQueuePool INSTANCE = null;

    private Map<String, RandomAccessTopicQueue> queueMap;

    public String zkRootPath;

    private ZkClient zkClient;

    private String filePath;

    private final Scheduler scheduler = new Scheduler(1, "heze-mq-topic-queue-", false);

    public synchronized static void startup(ZkClient zkClient, ServerConfig config) {
        if (INSTANCE == null)
            INSTANCE = new RandomAccessQueuePool(zkClient, config);
    }

    public synchronized static void destroy(){
        if (INSTANCE != null) {
            INSTANCE.disposal();
            INSTANCE = null;
        }
    }

    public synchronized static RandomAccessTopicQueue getQueue(String queueName) {
        if (INSTANCE.queueMap.containsKey(queueName))
            return INSTANCE.queueMap.get(queueName);
        return null;
    }

    public synchronized static RandomAccessTopicQueue getQueueOrCreate(String queueName){
        RandomAccessTopicQueue topicQueue;
        String fileDir = INSTANCE.filePath;
        topicQueue= INSTANCE.getQueueFromPool(INSTANCE.queueMap, queueName, fileDir);
        return topicQueue;
    }

    private RandomAccessQueuePool(ZkClient zkClient, ServerConfig config) {
        this.zkClient = zkClient;
        if (StringUtils.isBlank(config.getDataDir())) {
            this.filePath = DEFAULT_DATA_PATH;
        } else {
            this.filePath = config.getDataDir();
        }
        File fileDir = new File(this.filePath);
        if (!fileDir.exists()) {
            fileDir.mkdirs();
        }
        if (!fileDir.isDirectory() || !fileDir.canRead()) {
            throw new IllegalArgumentException(fileDir.getAbsolutePath() + " is not a readable log directory.");
        }

        this.zkRootPath= ZkUtils.ZK_BROKER_GROUP+"/"+config.getServerName();
        queueMap=scanDir(fileDir);
        LOGGER.info("RandomAccessQueuePool has been successfully started");
    }

    private Map<String, RandomAccessTopicQueue> scanDir(File pathDir) {
        Map<String, RandomAccessTopicQueue> existFQueues = new HashMap<>();
        Set<String> names=Arrays.stream(pathDir.listFiles((File dir, String name) ->
                name.startsWith("index") && name.endsWith(FILE_SUFFIX)))
                .map(file ->file.getName().split("_")[1]).collect(Collectors.toSet());
        if (CollectionUtils.isNotEmpty(names)) {
            for (String queueName:names) {
                existFQueues.put(queueName, new RandomAccessTopicQueue(queueName, pathDir.getAbsolutePath()));
                //register topic in zk. etc /brokerMQ/serverA/topic-1,/brokerMQ/serverA/topic-2
                zkClient.createPersistent(zkRootPath+"/"+queueName,false);
            }
        }
        return existFQueues;
    }

    private RandomAccessTopicQueue getQueueFromPool(Map<String, RandomAccessTopicQueue> queueMap, String queueName, String fileDir) {
        if (queueMap.containsKey(queueName))
            return queueMap.get(queueName);
        RandomAccessTopicQueue queue = new RandomAccessTopicQueue(queueName, fileDir);
        queueMap.put(queueName, queue);
        zkClient.createPersistent(zkRootPath+"/"+queueName,false);
        return queue;
    }


    private void disposal() {
        this.scheduler.shutdown();
        for (String key:queueMap.keySet()){
            queueMap.get(key).close();
            if (zkClient.exists(zkRootPath+"/"+key)){
                zkClient.delete(zkRootPath+"/"+key);
            }

        }
    }


}
