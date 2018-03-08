package com.yudy.heze.exception;

import org.apache.zookeeper.KeeperException;

public class ZkException extends RuntimeException{

    private static final long serialVersionUID=1L;

    public ZkException() {
        super();
    }

    public ZkException(String message, Throwable cause) {
        super(message, cause);
    }

    public ZkException(String message) {
        super(message);
    }

    public ZkException(Throwable cause) {
        super(cause);
    }

    public static ZkException create(KeeperException e){
        switch (e.code()){
            case NONODE:
                return new ZkNoNodeException(e);
            case BADVERSION:
                return new ZkBadVersionException(e);
            case NODEEXISTS:
                return new ZkNodeExistsException(e);
            default:
                return new ZkException(e);
        }
    }





}
