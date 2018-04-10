package com.yudy.heze.cluster;

import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

public class Cluster {

    private static final Queue<Group> MASTER_BROKER_GROUP = new LinkedBlockingDeque<Group>();
    private static final Queue<String> MASTER_BROKER_IP = new LinkedBlockingDeque<String>();
    private static final ConcurrentHashMap<String, List<Group>> SLAVE_BROKER = new ConcurrentHashMap<>();//<queueName,List>
    private static Group current;

    public static Group getCurrent() {
        return current;
    }

    public static void setCurrent(Group current){
        Cluster.current=current;
    }

    public static void addGroup(Group group){
        MASTER_BROKER_GROUP.add(group);
        MASTER_BROKER_IP.add(group.getMaster().getHost());
    }

    public static Group peek(){
        return MASTER_BROKER_GROUP.peek();
    }


    public static Queue<String> getMasterIps(){return MASTER_BROKER_IP;}

    public static void clear(){
        MASTER_BROKER_GROUP.clear();
        MASTER_BROKER_IP.clear();
        SLAVE_BROKER.clear();
        System.out.println("closing.....");
    }




}
