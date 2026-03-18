package com.koa.sws.constant;

public final class RedisKeyConstants {
    private RedisKeyConstants() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    private static final String PREFIX = "sws";

    public static final String PUBLISH_QUEUE = PREFIX + ":publishQueue";
    public static final String SUBSCRIBE_QUEUE = PREFIX + ":subscribeQueue";
    public static final String LOCK_PREFIX = PREFIX + ":lock:";
}
