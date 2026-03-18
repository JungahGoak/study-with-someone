package com.koa.sws.constant;

import java.util.concurrent.TimeUnit;

public final class DistributedLockConstants {
    private DistributedLockConstants() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    public static final long DEFAULT_WAIT_TIME_SECONDS = 5L;
    public static final long DEFAULT_LEASE_TIME_SECONDS = 3L;
    public static final TimeUnit DEFAULT_TIME_UNIT = TimeUnit.SECONDS;
    public static final String LOCK_PREFIX = "LOCK:";
}
