package com.yudy.heze.store;

import com.yudy.heze.config.ServerConfig;
import com.yudy.heze.server.backup.BackupQueuePool;
import com.yudy.heze.util.Scheduler;
import com.yudy.heze.util.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class BasicTopicQueuePool {

    private static final Logger LOGGER = LoggerFactory.getLogger(BasicTopicQueuePool.class);

    private static final String INDEX_FILE_SUFFIX = ".umq";

    public static final String DEFAULT_DATA_PATH = "./data";

    private static BasicTopicQueuePool INSTANCE = null;

    private Map<String, BasicTopicQueue> queueMap;

    private String zkRootPath;

    private ZkClient zkClient;

    private String filePath;

    private final Scheduler scheduler = new Scheduler(1, "heze-mq-topic-queue-", false);

    public synchronized static void startup(ZkClient zkClient, ServerConfig config) {
        if (INSTANCE == null)
            INSTANCE = new BasicTopicQueuePool(zkClient, config);
    }

    public synchronized static void destory() {
        if (INSTANCE != null) {
            INSTANCE.disposal();
            INSTANCE = null;
        }
    }

    public synchronized static BasicTopicQueue getQueue(String queueName) {
        if (INSTANCE.queueMap.containsKey(queueName))
            return INSTANCE.queueMap.get(queueName);
        return null;
    }

    public synchronized static BasicTopicQueue getQueueOrCreate(String queueName) {
        BasicTopicQueue topicQueue;
        String fileDir = INSTANCE.filePath;
        topicQueue= INSTANCE.getQueueFromPool(INSTANCE.queueMap, queueName, fileDir);
        return topicQueue;
    }

    private BasicTopicQueuePool(ZkClient zkClient, ServerConfig config) {
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
        queueMap=scanDir(fileDir,false);
        LOGGER.info("TopicQueuePool has been successfully started");
    }

    private Map<String, BasicTopicQueue> scanDir(File pathDir, boolean backup) {
        if (!pathDir.isDirectory()) {
            throw new IllegalArgumentException(pathDir.getAbsolutePath()+"is not a directory");
        }
        Map<String, BasicTopicQueue> existFQueues = new HashMap<>();
        File[] indexFiles = pathDir.listFiles((File dir, String name) -> name.startsWith("index")&&name.endsWith(INDEX_FILE_SUFFIX));
        if (ArrayUtils.isNotEmpty(indexFiles)) {
            for (File indexFile : indexFiles) {
                String queueName = parseQueueName(indexFile.getName());
                existFQueues.put(queueName, new BasicTopicQueue(queueName, pathDir.getAbsolutePath()));
                //register topic in zk. etc /brokerMQ/serverA/topic-1,/brokerMQ/serverA/topic-2
                zkClient.createPersistent(zkRootPath+"/"+queueName,false);
            }
        }
        return existFQueues;
    }

    private String parseQueueName(String indexFileName) {
        String fileName = indexFileName.substring(0, indexFileName.lastIndexOf('.'));
        return fileName.split("_")[1];
    }

    private void disposal() {
        this.scheduler.shutdown();
        for (String key:queueMap.keySet()){
            queueMap.get(key).close();
            if (zkClient.exists(zkRootPath+"/"+key)){
                zkClient.delete(zkRootPath+"/"+key);
            }

        }
//        queueMap.forEach((key,value)->{value.close();zkClient.delete(zkRootPath+"/"+key);});
    }

    private BasicTopicQueue getQueueFromPool(Map<String, BasicTopicQueue> queueMap, String queueName, String fileDir) {
        if (queueMap.containsKey(queueName))
            return queueMap.get(queueName);
        BasicTopicQueue queue = new BasicTopicQueue(queueName, fileDir);
        queueMap.put(queueName, queue);
        zkClient.createPersistent(zkRootPath+"/"+queueName,false);
        return queue;
    }
    
}
