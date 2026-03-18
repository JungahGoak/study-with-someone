package com.koa.sws.exception;

public class SignalingException extends RuntimeException {
    public SignalingException(String message) {
        super(message);
    }

    public SignalingException(String message, Throwable cause) {
        super(message, cause);
    }
}
