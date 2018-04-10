package com.yudy.heze.store;

import com.yudy.heze.config.ServerConfig;
import com.yudy.heze.server.backup.BackupQueuePool;
import com.yudy.heze.util.Scheduler;
import com.yudy.heze.zk.ZkClient;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

public class TopicQueuePool {

    private static final Logger LOGGER = LoggerFactory.getLogger(TopicQueuePool.class);

    public static final String INDEX_FILE_SUFFIX = ".umq";

    public static final String DEFAULT_DATA_PATH = "./data";
    public static final String DATA_BACKUP_PATH = "/backup";

    public static int ZK_SYNC_DATA_PERSISTENCE_INTERVAL = 3;

    private static final BlockingQueue<String> DELETING_QUEUE = new LinkedBlockingDeque<>();
    private static TopicQueuePool INSTANCE = null;
    private final ZkClient zkClient;
    private final String filePath;
    private Map<String, TopicQueue> queueMap;
    private final Scheduler scheduler = new Scheduler(1, "uncode-mq-topic-queue-", false);


    private TopicQueuePool(ZkClient zkClient, ServerConfig config) {
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
        File fileBackupDir = new File(this.filePath + DATA_BACKUP_PATH);
        if (!fileBackupDir.exists()) {
            fileBackupDir.mkdirs();
        }
        if (!fileBackupDir.isDirectory() || !fileBackupDir.canRead()) {
            throw new IllegalArgumentException(fileBackupDir.getAbsolutePath() + " is not a readable log directory.");
        }
        queueMap=scanDir(fileDir,false);
        long delay = config.getDataPersistenceInterval();
        ZK_SYNC_DATA_PERSISTENCE_INTERVAL = (int) (config.getZKDataPersistenceInterval() / config.getDataPersistenceInterval());
        scheduler.scheduleWithRate(()-> {
            queueMap.values().forEach(TopicQueue::sync);
            deleteBlockFile();
        },1000L, delay * 1000L);
        LOGGER.info("TopicQueuePool has been successfully started");
    }

    private void deleteBlockFile() {
        String blockFilePath = DELETING_QUEUE.poll();
        if (StringUtils.isNotBlank(blockFilePath)) {
            File delFile = new File(blockFilePath);
            try {
                if (!delFile.delete()) {
                    LOGGER.warn("block file:{} delete failed", blockFilePath);
                }
            } catch (SecurityException e) {
                LOGGER.error("security manager exists, delete denied");
            }
        }
    }

    public static void toClear(String filePath) {
        DELETING_QUEUE.add(filePath);
    }

    private Map<String, TopicQueue> scanDir(File pathDir, boolean backup) {
        if (!pathDir.isDirectory()) {
            throw new IllegalArgumentException("it is not a directory");
        }
        Map<String, TopicQueue> existFQueues = new HashMap<>();
        File[] indexFiles = pathDir.listFiles((File dir, String name) -> name.endsWith(INDEX_FILE_SUFFIX));
        if (ArrayUtils.isNotEmpty(indexFiles)) {
            for (File indexFile : indexFiles) {
                String queueName = parseQueueName(indexFile.getName());
                existFQueues.put(queueName, new TopicQueue(queueName, pathDir.getAbsolutePath(), this.zkClient, backup));
            }
        }
        return existFQueues;
    }

    public static String parseQueueName(String indexFileName) {
        String fileName = indexFileName.substring(0, indexFileName.lastIndexOf('.'));
        return fileName.split("_")[1];
    }

    public synchronized static void startup(ZkClient zkClient, ServerConfig config) {
        if (INSTANCE == null)
            INSTANCE = new TopicQueuePool(zkClient, config);
        //todo
//        BackupQueuePool.
    }

    public synchronized static void startup(ServerConfig config) {
        startup(null, config);
    }

    private void disposal() {
        this.scheduler.shutdown();
        for (TopicQueue queue : queueMap.values()) {
            queue.close();
        }
        while (!DELETING_QUEUE.isEmpty()) {
            deleteBlockFile();
        }
    }

    public synchronized static void destory() {
        if (INSTANCE != null) {
            INSTANCE.disposal();
            INSTANCE = null;
        }
    }

    public synchronized static TopicQueue getQueue(String queueName) {
        if (StringUtils.isBlank(queueName)) {
            throw new IllegalArgumentException("emmpty queue name");
        }
        if (INSTANCE.queueMap.containsKey(queueName))
            return INSTANCE.queueMap.get(queueName);
        return null;
    }

    public synchronized static TopicQueue getQueueOrCreate(String queueName) {
        TopicQueue topicQueue = null;
        if (StringUtils.isBlank(queueName)) {
            throw new IllegalArgumentException("emmpty queue name");
        }
        String fileDir = INSTANCE.filePath;
        topicQueue= INSTANCE.getQueueFromPool(INSTANCE.queueMap, queueName, fileDir);
        return topicQueue;
    }

    private TopicQueue getQueueFromPool(Map<String, TopicQueue> queueMap, String queueName, String fileDir) {
        if (queueMap.containsKey(queueName))
            return queueMap.get(queueName);
        TopicQueue queue = new TopicQueue(queueName, fileDir, zkClient, false);
        queueMap.put(queueName, queue);
        return queue;
    }

    public static Set<String> getQueueNameFromDisk() {
        Set<String> names = null;
        File fileDir = new File(INSTANCE.filePath);
        if (fileDir.exists() && fileDir.isDirectory() && fileDir.canRead()) {
            File[] indexFiles = fileDir.listFiles((File dir, String name) -> (name.endsWith(INDEX_FILE_SUFFIX)));
            if (ArrayUtils.isNotEmpty(indexFiles)) {
                names = new HashSet<>();
                for (File indexFile : indexFiles) {
                    String[] nas = indexFile.getName().split("_");
                    if (nas != null && nas.length > 1) {
                        names.add(nas[1].replace(INDEX_FILE_SUFFIX, ""));
                    }
                }
            }
        }
        return names;
    }
}