package com.yudy.heze.exception;

public class SendRequestException extends RuntimeException {
    /**
     *
     */
    private static final long serialVersionUID = 1L;

    public SendRequestException(String message) {
        super(message);
    }

    public SendRequestException(String message, Throwable cause) {
        super(message, cause);
    }

    public SendRequestException(Throwable cause) {
        super(cause);
    }
}