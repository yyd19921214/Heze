package com.yudy.heze.cluster;

import org.apache.commons.lang.StringUtils;

public class Group {

    public static final String QUEUE_INDEX_PREFIX = "ms-";

    private static final long serialVersionUID = 1L;

    private String name;

    private Broker master;

    private Broker slaveOf;

    public Group(){}


    public Group(String name,String hostname,int port){this(name,hostname,port,null);}


    public Group(String name,String hostname,int port,String replicaHost){
        String groupName="ser";
        if (StringUtils.isNotBlank(name)){
            groupName=name;
        }
        this.name=groupName;
        this.master=new Broker(hostname,port);
        if (StringUtils.isNotBlank(replicaHost)){
            this.slaveOf=new Broker(replicaHost,port);
        }
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Broker getMaster() {
        return master;
    }

    public void setMaster(Broker master) {
        this.master = master;
    }

    public Broker getSlaveOf() {
        return slaveOf;
    }

    public void setSlaveOf(Broker slaveOf) {
        this.slaveOf = slaveOf;
    }

    public Group clone(){
        Group group=null;
        if (this.slaveOf==null)
            group=new Group(name,master.getHost(),master.getPort());
        else
            group=new Group(name,master.getHost(),master.getPort(),slaveOf.getHost());
        group.getMaster().setShost(master.getShost());
        return group;
    }

    public String getZkIndexMasterSlave(){
        StringBuffer sb=new StringBuffer();
        if (master!=null&&StringUtils.isNotBlank(master.getHost())){
            sb.append(master.getHost()).append(":");
        }
        if (slaveOf!=null&&StringUtils.isNotBlank(slaveOf.getHost())){
            sb.append(slaveOf.getHost()).append(":");
        }
        sb.deleteCharAt(sb.lastIndexOf(":"));
        return sb.toString();
    }


    @Override
    public String toString() {
        return String.format("name:%s,master:[%s],slaveOf:[%s]", name, master==null?"":master.toString(), slaveOf==null?"":slaveOf.toString());

    }
}
