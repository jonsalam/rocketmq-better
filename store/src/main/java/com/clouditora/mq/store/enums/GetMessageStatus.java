package com.clouditora.mq.store.enums;

/**
 * @link org.apache.rocketmq.store.GetMessageStatus
 */
public enum GetMessageStatus {
    FOUND,
    NO_MATCHED_MESSAGE,
    MESSAGE_WAS_REMOVING,
    OFFSET_FOUND_NULL,
    OFFSET_TOO_SMALL,
    /**
     * @link org.apache.rocketmq.store.GetMessageStatus#OFFSET_OVERFLOW_ONE
     */
    OFFSET_OVER,
    /**
     * @link org.apache.rocketmq.store.GetMessageStatus#OFFSET_OVERFLOW_BADLY
     */
    OFFSET_OVERFLOW,
    NO_MATCHED_LOGIC_QUEUE,
    NO_MESSAGE_IN_QUEUE,
}
