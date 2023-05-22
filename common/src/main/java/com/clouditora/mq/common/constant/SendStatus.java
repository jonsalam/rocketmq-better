package com.clouditora.mq.common.constant;

/**
 * @link org.apache.rocketmq.client.producer.SendStatus
 */
public enum SendStatus {
    SEND_OK,
    FLUSH_DISK_TIMEOUT,
    FLUSH_SLAVE_TIMEOUT,
    SLAVE_NOT_AVAILABLE,
}
