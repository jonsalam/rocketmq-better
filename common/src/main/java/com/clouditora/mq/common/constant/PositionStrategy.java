package com.clouditora.mq.common.constant;

/**
 * @link org.apache.rocketmq.common.consumer.ConsumeFromWhere
 */
public enum PositionStrategy {
    CONSUME_FROM_FIRST_OFFSET,
    CONSUME_FROM_LAST_OFFSET,
    CONSUME_FROM_TIMESTAMP,
}
