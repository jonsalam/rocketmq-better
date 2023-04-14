package com.clouditora.mq.client.util;

import lombok.Data;

import java.util.Random;

/**
 * @link org.apache.rocketmq.client.common.ThreadLocalIndex
 */
@Data
public class ThreadLocalIndex {
    private final static int POSITIVE_MASK = 0x7FFFFFFF;

    private final ThreadLocal<Integer> threadLocalIndex = ThreadLocal.withInitial(() -> Math.abs(new Random().nextInt()));

    public int incrementAndGet() {
        Integer index = this.threadLocalIndex.get();
        this.threadLocalIndex.set(++index);
        return Math.abs(index & POSITIVE_MASK);
    }
}
