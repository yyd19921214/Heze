package com.yudy.heze.util;

import com.yudy.heze.cluster.Cluster;
import com.yudy.heze.cluster.Group;
import com.yudy.heze.exception.ZkNoNodeException;
import com.yudy.heze.server.ServerRegister;
import com.yudy.heze.zk.ZkClient;
import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

public class ZkUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(ZkUtils.class);

    public static final String ZK_MQ_BASE = "/HEZEMQ";

    public static final String ZK_BROKER_GROUP = ZK_MQ_BASE + "/brokergroup";

    public static String makeSurePersistentPathExist(ZkClient zkClient, String path) {
        String s;
        if (!zkClient.exists(path, true)) {
            s=zkClient.createPersistent(path, true);
            return s;
        }
        return path;
    }

    public static List<String> getChildrenParentMayNotExist(ZkClient zkClient, String path) {
        try {
            return zkClient.getChildren(path);
        } catch (ZkNoNodeException e) {
            return null;
        }
    }

    public static String readData(ZkClient zkClient, String path) {
        return fromByte(zkClient.readData(path));
    }


    private static void createParentPath(ZkClient zkClient, String path) {
        String parentDir = path.substring(0, path.lastIndexOf('/'));
        if (parentDir.length() != 0) {
            zkClient.createPersistent(parentDir, true);
        }
    }

    public static String createEphemeralPath(ZkClient zkClient,String path,String data){
        String pathCT;
        try{
            pathCT=zkClient.createEphemeralSequential(path,getBytes(data));
        }catch (ZkNoNodeException e){
            createParentPath(zkClient,path);
            pathCT=zkClient.createEphemeralSequential(path,getBytes(data));
        }
        return pathCT;
    }

    public static String createEphemeralPathExpectConflict(ZkClient zkClient,String path,String data){
        return createEphemeralPath(zkClient,path,data);
    }

    private static String fromByte(byte[] bytes) {
        return fromByte(bytes, "utf-8");
    }


    private static String fromByte(byte[] bytes, String encode) {
        if (bytes == null)
            return null;
        try {
            String s = new String(bytes, encode);
            return s;
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static byte[] getBytes(String s) {
        return getBytes(s, "utf-8");
    }

    private static byte[] getBytes(String s, String encode) {
        if (s == null)
            return null;
        try {
            return s.getBytes(encode);
        } catch (UnsupportedEncodingException e) {
            return s.getBytes();
        }
    }

    public static void getCluster(ZkClient zkClient) {
        try {
            if (zkClient.getZooKeeper().getState().isAlive()) {
                List<String> allGroupsName = ZkUtils.getChildrenParentMayNotExist(zkClient, ServerRegister.ZK_BROKER_GROUP);
                if (allGroupsName != null) {
                    List<Group> allGroup = new ArrayList<>();
                    for (String group : allGroupsName) {
                        String jsonGroup = ZkUtils.readData(zkClient, ServerRegister.ZK_BROKER_GROUP + "/" + group);
                        if (StringUtils.isNotBlank(jsonGroup)) {
                            Group groupObj = DataUtils.json2BrokerGroup(jsonGroup);
                            allGroup.add(groupObj);
                        }
                    }
                    Cluster.clear();
                    allGroup.forEach(group -> {
                        if (group.getSlaveOf() != null) {
                            group.getMaster().setShost(group.getSlaveOf().getHost());
                        }
                        Cluster.addGroup(group);
                    });
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.error("get cluster error", e);
        }
    }




}
