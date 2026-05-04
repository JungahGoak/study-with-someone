package com.koa.sws.constant;

import java.time.Duration;

public final class RedisKeyConstants {
    private RedisKeyConstants() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    private static final String PREFIX = "sws";

    public static final String PUBLISH_QUEUE = PREFIX + ":publishQueue";
    public static final String SUBSCRIBE_QUEUE = PREFIX + ":subscribeQueue";

    public static final String PEER_PREFIX = PREFIX + ":peer:";
    public static final String PUBLISHER_SUFFIX = ":publisher";
    public static final String SUBSCRIBER_SUFFIX = ":subscriber";
    public static final Duration PEER_SESSION_TTL = Duration.ofHours(24);
}
