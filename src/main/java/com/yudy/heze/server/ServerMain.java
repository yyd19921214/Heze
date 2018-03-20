package com.yudy.heze.server;

import com.yudy.heze.config.ServerConfig;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ServerMain {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServerMain.class);

    private static final String PARAM_KEY_PORT = "-p";
    private static final String PARAM_KEY_REPLICA = "-replica";
    private static final String PARAM_KEY_LOG_DIR = "-dir";
    private static final String PARAM_KEY_HOST = "-h";
    private static final String PARAM_KEY_CFG = "-cfg";
    private static final String PARAM_KEY_INFO = "-info";

    public static void main(String[] args) {

        int port=9999;
        String replica=null;
        String dir=null;
        String host="127.0.0.1";
        String cfg=null;
        String info=null;

        int argSize=args.length;
        if (argSize>0){

            if (argSize%2!=0){
                LOGGER.error("number of arguments is wrong");
                System.out.println("number of arguments is wrong");
                System.exit(1);
            }
            int i=0;
            while (i<argSize){
                if (StringUtils.isBlank(args[i])||StringUtils.isBlank(args[i+1]))
                    continue;
                if (PARAM_KEY_PORT.equals(args[i].toLowerCase())){
                    port=Integer.valueOf(args[i+1].trim());
                }
                else if (PARAM_KEY_REPLICA.equals(args[i].toLowerCase())){
                    replica=String.valueOf(args[i+1].trim());
                }
                else if (PARAM_KEY_LOG_DIR.equals(args[i].toLowerCase())){
                    dir=String.valueOf(args[i+1].trim());
                }
                else if (PARAM_KEY_HOST.equals(args[i].toLowerCase())){
                    host=String.valueOf(args[i+1].trim());
                }
                else if (PARAM_KEY_CFG.equals(args[i].toLowerCase())){
                    cfg=String.valueOf(args[i+1].trim());
                }
                else if (PARAM_KEY_INFO.equals(args[i].toLowerCase())){
                    info=String.valueOf(args[i+1].trim());
                }
                i+=2;
            }
            ServerConfig config=null;
            if (StringUtils.isNotBlank(cfg)){
                config=new ServerConfig(cfg);
            }
            else{
                Properties cfgProp=new Properties();
                cfgProp.setProperty("port",port+"");
                cfgProp.setProperty("host",host);
                if (StringUtils.isNotBlank(replica)){
                    cfgProp.setProperty("replica.host",replica);
                }
                if (StringUtils.isNotBlank(dir)){
                    cfgProp.setProperty("log.dir",dir);
                }
                config=new ServerConfig(cfgProp);
            }
            NettyServer nettyServer=new NettyServer();
            nettyServer.start(config);




        }



    }


}
