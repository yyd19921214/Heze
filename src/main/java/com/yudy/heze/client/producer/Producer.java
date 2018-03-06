package com.yudy.heze.client.producer;

import com.yudy.heze.client.NettyClient;
import com.yudy.heze.config.ServerConfig;
import com.yudy.heze.network.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class Producer {

    private static final Producer INSTANCE = new Producer();

    private final static Logger LOGGER = LoggerFactory.getLogger(Producer.class);

    private NettyClient client=null;

    private BlockingQueue<Topic> errorQueue=new LinkedBlockingQueue<>();

    private Producer(){}

    public static Producer getInstance(){
        return INSTANCE;
    }

    public void connect(String path){
        if (client==null){
            File mainFile=null;

            try {
                URL url=new URL(path);
                mainFile=new File(url.getFile()).getCanonicalFile();
            } catch (MalformedURLException e) {
                e.printStackTrace();
            } catch (IOException e) {

                System.exit(2);
                e.printStackTrace();
            }

            final ServerConfig config=new ServerConfig(mainFile);


        }
    }

    public void connect(ServerConfig config){
        if (client==null){
            client=new NettyClient();
            if (config.getEnableZookeeper()){
                loadClusterFromZK(config);
//                client.zkClient

            }
            else{

            }
        }

    }

    private void loadClusterFromZK(ServerConfig config){
        client.initZKClient(config);
        if(config.getEnableZookeeper()){
            //TODO

        }

    }






}
