package com.clouditora.mq.store.file;

/**
 * @link org.apache.rocketmq.store.PutMessageStatus
 */
public enum AppendStatus {
    SUCCESS,
    UNKNOWN_ERROR,
    FLUSH_DISK_TIMEOUT,
    FLUSH_SLAVE_TIMEOUT,
    SLAVE_NOT_AVAILABLE,
    SERVICE_NOT_AVAILABLE,
    CREATE_MAPPED_FILE_FAILED,
    MESSAGE_ILLEGAL,
    OS_PAGE_CACHE_BUSY,
}
