package com.yudy.heze.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;

public class PortUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(PortUtils.class);

    public static int checkAvailablePort(int port){
        ServerSocket serverSocket=null;

        try {
            serverSocket=new ServerSocket(port);

        } catch (IOException e) {
            throw new RuntimeException(e.getMessage() + String.format(" port %d", port), e);
        }finally {
            if (serverSocket!=null){
                try {
                    serverSocket.close();
                } catch (IOException e) {
                    LOGGER.error(e.getMessage());
                    e.printStackTrace();
                }
            }
        }
        return port;
    }

    public static void main(String[] args) {
        int port = checkAvailablePort(808);
        System.out.println("The available port is " + port);
    }


}
