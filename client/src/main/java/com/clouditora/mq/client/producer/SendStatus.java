package com.clouditora.mq.client.producer;

/**
 * @link org.apache.rocketmq.client.producer.SendStatus
 */
public enum SendStatus {
    SEND_OK,
    FLUSH_DISK_TIMEOUT,
    FLUSH_SLAVE_TIMEOUT,
    SLAVE_NOT_AVAILABLE,
}
