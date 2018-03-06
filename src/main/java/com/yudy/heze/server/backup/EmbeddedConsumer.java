package com.yudy.heze.server.backup;

import com.yudy.heze.config.ServerConfig;

public class EmbeddedConsumer {

    private static final EmbeddedConsumer INSTANCE = new EmbeddedConsumer();

    public static EmbeddedConsumer getInstance(){
        return INSTANCE;
    }

    public void start(ServerConfig config) {

    }

}
