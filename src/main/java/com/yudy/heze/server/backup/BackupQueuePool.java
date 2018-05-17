package com.yudy.heze.server.backup;

import com.yudy.heze.config.ServerConfig;
import com.yudy.heze.util.Scheduler;
import com.yudy.heze.zk.ZkClient;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;

import static com.yudy.heze.store.pool.TopicQueuePool.*;

public class BackupQueuePool {

    private static final Logger LOGGER=LoggerFactory.getLogger(BackupQueuePool.class);

    private static BackupQueuePool INSTANCE=null;
    private String filePath;
    private Map<String,BackupQueue> backupQueueMap;
    private final Scheduler scheduler=new Scheduler(1,"heze-mq-backup-queue-",false);
    private ZkClient zkClient;

    private BackupQueuePool(ZkClient zkClient, ServerConfig config){
        this.zkClient=zkClient;
        this.filePath=DEFAULT_DATA_PATH + DATA_BACKUP_PATH;
        if (StringUtils.isNotBlank(config.getDataDir())){
            this.filePath=config.getDataDir()+DATA_BACKUP_PATH;
        }
        File fileDir=new File(this.filePath);
        if (!fileDir.exists())
            fileDir.mkdirs();
        if (!fileDir.isDirectory()||!fileDir.canRead()){
            LOGGER.error(fileDir.getAbsolutePath() + " is not a readable log directory.");
            throw new IllegalArgumentException(fileDir.getAbsolutePath() + " is not a readable log directory.");
        }
        this.backupQueueMap = scanDir(fileDir);
        this.scheduler.scheduleWithRate(()->{
            backupQueueMap.values().stream().forEach(q->q.sync());
        },1000L,4000L);
    }

    private Map<String,BackupQueue> scanDir(File pathDir){
        if (!pathDir.isDirectory()){
            LOGGER.error(pathDir.getAbsolutePath()+"it is not a directory");
            throw new IllegalArgumentException("it is not a directory");
        }
        Map<String,BackupQueue> existQueue=new HashMap<>();
        File[] indexFiles=pathDir.listFiles((dir, name) -> name.endsWith(INDEX_FILE_SUFFIX));
        if (ArrayUtils.isNotEmpty(indexFiles)){
            Arrays.stream(indexFiles).forEach(file->{
                String queueName=parseQueueName(file.getName());
                existQueue.put(queueName,new BackupQueue(queueName,pathDir.getAbsolutePath(),zkClient));
            });
        }
        return existQueue;
    }

    private String parseQueueName(String indexFileName) {
        String fileName = indexFileName.substring(0, indexFileName.lastIndexOf('.'));
        return fileName.split("_")[1];
    }

    public synchronized static void startup(ZkClient zkClient,ServerConfig config){
        if (INSTANCE==null){
            INSTANCE=new BackupQueuePool(zkClient,config);
        }
    }

    public synchronized static BackupQueue getBackupQueueFromPool(String queueName)
    {
        if (INSTANCE.backupQueueMap.containsKey(queueName)){
            return INSTANCE.backupQueueMap.get(queueName);
        }
        else{
            BackupQueue queue=new BackupQueue(queueName,INSTANCE.filePath,INSTANCE.zkClient);
            INSTANCE.backupQueueMap.put(queueName,queue);
            return queue;
        }
    }

    public synchronized static Set<String> getBackupQueueNameFromDisk(){
        Set<String> names=new HashSet<>();
        File fileDir=new File(INSTANCE.filePath);
        if (fileDir.exists()&&fileDir.isDirectory()&&fileDir.canRead()){
            File[] indexFiles=fileDir.listFiles((dir, name) -> name.endsWith(INDEX_FILE_SUFFIX)&&name.startsWith("tindex"));
            if (ArrayUtils.isNotEmpty(indexFiles)){

                Arrays.stream(indexFiles).forEach(file->{
                    String[] nas=file.getName().split("_");
                    if (nas!=null&&nas.length>1){
                        names.add(nas[1].replace(INDEX_FILE_SUFFIX, ""));
                    }
                });

            }
        }
        return names;
    }

}
