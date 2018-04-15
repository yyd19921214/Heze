package com.yudy.heze.server;

import java.io.Closeable;
import java.io.IOException;

public interface MServer extends Closeable{

    boolean startup(String configName);

    @Override
    void close() throws IOException;


}
