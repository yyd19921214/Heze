package com.yudy.heze.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;

public class Closer {

    private static final Logger LOGGER = LoggerFactory.getLogger(Closer.class);

    public static void closeQuietly(Closeable closeable,Logger logger){
        if (closeable==null)
            return;
        try {
            closeable.close();
        } catch (IOException e) {
            logger.error(e.getMessage(),e);
        }
    }

    public static void closeQuietly(Closeable closeable){
        closeQuietly(closeable,LOGGER);
    }
}
