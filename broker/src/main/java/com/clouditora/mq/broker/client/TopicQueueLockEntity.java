package com.clouditora.mq.broker.client;

import lombok.Getter;
import lombok.Setter;

public class TopicQueueLockEntity {
    /**
     * 客户端的锁是30S, 这里是60S
     */
    private final static long MAX_LIVE_TIME = Long.parseLong(System.getProperty("rocketmq.broker.rebalance.lockMaxLiveTime", "60000"));

    @Getter
    private final String clientId;
    @Setter
    private volatile long timestamp = System.currentTimeMillis();

    public TopicQueueLockEntity(String clientId) {
        this.clientId = clientId;
    }

    public boolean isExpired() {
        return System.currentTimeMillis() - timestamp > MAX_LIVE_TIME;
    }

    public boolean isLocked(String clientId) {
        return this.clientId.equals(clientId);
    }

    public void updateTimestamp() {
        this.timestamp = System.currentTimeMillis();
    }
}
