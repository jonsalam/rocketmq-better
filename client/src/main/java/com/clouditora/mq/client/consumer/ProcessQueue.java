package com.clouditora.mq.client.consumer;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @link org.apache.rocketmq.client.impl.consumer.ProcessQueue
 */
@Slf4j
public class ProcessQueue {
    public final static long REBALANCE_LOCK_MAX_LIVE_TIME = Long.parseLong(System.getProperty("rocketmq.client.rebalance.lockMaxLiveTime", "30000"));
    public final static long REBALANCE_LOCK_INTERVAL = Long.parseLong(System.getProperty("rocketmq.client.rebalance.lockInterval", "20000"));
    private final static long PULL_MAX_IDLE_TIME = Long.parseLong(System.getProperty("rocketmq.client.pull.pullMaxIdleTime", "120000"));

    @Setter
//    private volatile long lastPullTimestamp = System.currentTimeMillis();
    private final AtomicLong messageCount = new AtomicLong();
    private final AtomicLong messageSize = new AtomicLong();

    public long getMessageCount() {
        return this.messageCount.get();
    }

    public long getMessageSize() {
        return this.messageSize.get();
    }

    public long getMaxSpan() {
        try {
            this.treeMapLock.readLock().lockInterruptibly();
            try {
                if (!this.msgTreeMap.isEmpty()) {
                    return this.msgTreeMap.lastKey() - this.msgTreeMap.firstKey();
                }
            } finally {
                this.treeMapLock.readLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("getMaxSpan exception", e);
        }

        return 0;
    }
}
