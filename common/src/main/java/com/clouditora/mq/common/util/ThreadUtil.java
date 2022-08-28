package com.clouditora.mq.common.util;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
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

    public static ThreadPoolExecutor newFixedThreadPool(int poolSize, String threadName) {
        return new ThreadPoolExecutor(
                poolSize, poolSize,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                ThreadUtil.buildFactory(threadName, poolSize)
        );
    }
}
