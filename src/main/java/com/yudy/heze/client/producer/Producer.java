package com.yudy.heze.client.producer;

import com.yudy.heze.client.NettyClient;
import com.yudy.heze.cluster.Broker;
import com.yudy.heze.cluster.Cluster;
import com.yudy.heze.cluster.Group;
import com.yudy.heze.config.ServerConfig;
import com.yudy.heze.exception.SendRequestException;
import com.yudy.heze.network.Message;
import com.yudy.heze.network.Topic;
import com.yudy.heze.network.TransferType;
import com.yudy.heze.server.RequestHandler;
import com.yudy.heze.util.DataUtils;
import com.yudy.heze.util.ZkUtils;
import org.apache.zookeeper.ZKUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class Producer {

    private static final Producer INSTANCE = new Producer();

    private final static Logger LOGGER = LoggerFactory.getLogger(Producer.class);

    private NettyClient client = null;

    private BlockingQueue<Topic> errorQueue = new LinkedBlockingQueue<>();

    private Producer() {
    }

    public static Producer getInstance() {
        return INSTANCE;
    }

    public void connect(String path) {
        if (client == null) {
            File mainFile = null;
            try {
                URL url = new URL(path);
                mainFile = new File(url.getFile()).getCanonicalFile();
            } catch (IOException e) {
                System.exit(2);
                e.printStackTrace();
            }
            if (!mainFile.isFile() || !mainFile.exists()) {
                LOGGER.error(String.format("ERROR: Main config file not exist => '%s', copy one from 'conf/server.properties.sample' first.", mainFile.getAbsolutePath()));
                System.exit(2);
            }
            final ServerConfig config = new ServerConfig(mainFile);
            connect(config);
        }
    }


    public void connect(ServerConfig config) {
        if (client == null) {
            client = new NettyClient();
            if (config.getEnableZookeeper()) {
                loadClusterFromZK(config);

            } else {
                Group brokerGroup=new Group(config.getBrokerGroupName(),config.getHost(),config.getPort());
                Cluster.setCurrent(brokerGroup);
                //TODO may be removed after refined
                //add this just to support condition of zk is unabled
                if (config.getEnableZookeeper()!=true){
                    Cluster.addGroup(brokerGroup);
                }
            }
        }

    }

    public boolean reconnect(){
        int count=0;
        do{
            Group group=Cluster.peek();
            if (null==group)
                return false;
            Broker master=group.getMaster();
            try{
                client.open(master.getHost(),master.getPort());
                break;
            }catch (IllegalStateException e){
                client.stop();
                client=new NettyClient();
                count++;
                //todo enable zk
            }catch (Exception e){
                count++;
                //todo enable zk
            }

        }while (count<2 && !client.isConnected());

        return client.isConnected();
    }



    public boolean send(Topic topic){
        return send(new Topic[]{topic});

    }

    private boolean send(Topic[] topics){
        return send(Arrays.asList(topics));
    }

    public boolean send(Topic topic,String... topicNames){
        List<Topic> topics=new ArrayList<>();
        if (null!=topicNames){
            for (String tp:topicNames){
                Topic top=new Topic();
                top.setTopic(tp);
                top.getContents().addAll(topic.getContents());
                topics.add(top);
            }
        }
        return send(topics);
    }

    private boolean send(List<Topic> topics){
        boolean result=false;
        if (reconnect()){
            Message request=Message.newRequestMessage();
            request.setReqHandlerType(RequestHandler.PRODUCER);
            request.setBody(DataUtils.serialize(topics));
            boolean errorFlag=false;
            try{
                Message response=client.write(request);
                if (response==null||response.getType()== TransferType.EXCEPTION.value){
                    errorQueue.addAll(topics);
                    result=false;
                    errorFlag=true;
                }
                else{
                    result=true;
                }
            }catch (com.yudy.heze.exception.TimeoutException|SendRequestException e){
                e.printStackTrace();
                client=new NettyClient();
                errorQueue.addAll(topics);
                errorFlag=true;
                if (!reconnect()){
                    throw new SendRequestException("Producer connection error");
                }
            }

            if (!errorFlag){
                if (errorQueue.size()>0){
                    try {
                        Topic t=errorQueue.poll(100, TimeUnit.MILLISECONDS);
                        send(t);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        else {
            errorQueue.addAll(topics);
        }
        return result;
    }

    private void loadClusterFromZK(ServerConfig config) {
        client.initZKClient(config);
        if (config.getEnableZookeeper()) {
            ZkUtils.getCluster(client.zkClient);
        }
    }


}
