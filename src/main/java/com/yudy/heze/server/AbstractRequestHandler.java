package com.yudy.heze.server;

import com.yudy.heze.config.ServerConfig;

public abstract class AbstractRequestHandler implements RequestHandler{

    protected ServerConfig config;

    public AbstractRequestHandler(ServerConfig config){
        this.config = config;
    }


}
