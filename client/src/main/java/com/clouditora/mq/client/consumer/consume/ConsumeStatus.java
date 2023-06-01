package com.clouditora.mq.client.consumer.consume;

/**
 * @link org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus
 */
public enum ConsumeStatus {
    /**
     * Success consumption
     */
    CONSUME_SUCCESS,
    /**
     * Failure consumption, later try to consume
     */
    RECONSUME_LATER;
}
