package com.yudy.heze.config;

import java.io.File;
import java.util.Properties;

public class ServerConfig extends Config{

    public ServerConfig(Properties props) {
        super(props);
    }

    public ServerConfig(String filename){
        super(filename);
    }

    public ServerConfig(File cfg) {
        super(cfg);
    }

}
