package com.clouditora.mq.common.util;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class ThreadUtil {
    public static ThreadFactory buildFactory(String threadName, int maxCount) {
        return new ThreadFactory() {
            private final AtomicInteger counter = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, String.format("%s:%d/%d", threadName, counter.incrementAndGet(), maxCount));
            }
        };
    }
}
