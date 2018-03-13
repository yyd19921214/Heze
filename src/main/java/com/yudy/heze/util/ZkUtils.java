package com.yudy.heze.util;

import com.yudy.heze.exception.ZkNoNodeException;
import com.yudy.heze.zk.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.List;

public class ZkUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(ZkUtils.class);

    public static final String ZK_MQ_BASE="/HEZEMQ";

    public static void makeSurePersistentPathExist(ZkClient zkClient,String path){
        if (!zkClient.exists(path,true)){
            try {
                zkClient.createPersistent(path,true);
            }catch (Exception e){
                zkClient.delete(path);
                zkClient.createPersistent(path,true);
            }
        }
    }

    public static List<String> getChildrenParentMayNotExist(ZkClient zkClient, String path) {
        try{
            return zkClient.getChildren(path);
        }catch (ZkNoNodeException e){
            return null;
        }
    }

    public static String readData(ZkClient zkClient,String path){
        return fromByte(zkClient.readData(path));
    }

    public static String readDataMaybeNull(ZkClient zkClient,String path){
        try{
            return fromByte(zkClient.readData(path));
        }catch (Exception e){
            return null;
        }
    }

    private static void createParentPath(ZkClient zkClient, String path) {
        String parentDir = path.substring(0, path.lastIndexOf('/'));
        if (parentDir.length() != 0) {
            zkClient.createPersistent(parentDir, true);
        }
    }

    public static String fromByte(byte[] bytes){
        return fromByte(bytes,"utf-8");
    }


    public static String fromByte(byte[] bytes,String encode){
        if (bytes==null)
            return null;
        try {
            String s=new String(bytes,encode);
            return s;
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static byte[] getBytes(String s){
        return getBytes(s,"utf-8");
    }

    public static byte[] getBytes(String s,String encode){
        if (s==null)
            return null;
        try {
            return s.getBytes(encode);
        } catch (UnsupportedEncodingException e) {
            return s.getBytes();
        }
    }







}
