package com.koa.sws.exception;

public class SessionException extends SignalingException {
    public SessionException(String message) {
        super(message);
    }

    public SessionException(String message, Throwable cause) {
        super(message, cause);
    }
}
