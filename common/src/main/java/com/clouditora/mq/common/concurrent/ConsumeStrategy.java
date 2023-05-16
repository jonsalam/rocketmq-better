package com.clouditora.mq.common.concurrent;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * @link org.apache.rocketmq.common.protocol.heartbeat.ConsumeType
 */
@Getter
@AllArgsConstructor
public enum ConsumeStrategy {
    /**
     * @link org.apache.rocketmq.common.protocol.heartbeat.ConsumeType#CONSUME_ACTIVELY
     */
    PULL("PULL"),
    /**
     * @link org.apache.rocketmq.common.protocol.heartbeat.ConsumeType#CONSUME_PASSIVELY
     */
    PUSH("PUSH");

    private final String strategy;
}
