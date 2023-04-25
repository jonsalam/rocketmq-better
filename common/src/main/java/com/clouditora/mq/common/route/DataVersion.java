package com.clouditora.mq.common.route;

import lombok.Data;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @link org.apache.rocketmq.common.DataVersion
 */
@Data
public class DataVersion {
    private long timestamp = 0;
    private AtomicLong counter = new AtomicLong(0);
}
