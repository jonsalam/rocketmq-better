package com.clouditora.mq.common.constant;

/**
 * @link org.apache.rocketmq.common.consumer.ConsumeFromWhere
 */
public enum ConsumePositionStrategy {
    FROM_FIRST_OFFSET,
    FROM_LAST_OFFSET,
    FROM_TIMESTAMP,
}
