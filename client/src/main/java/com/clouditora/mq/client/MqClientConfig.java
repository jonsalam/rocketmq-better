package com.clouditora.mq.client;

import lombok.Data;

/**
 * @link org.apache.rocketmq.client.ClientConfig
 */
@Data
public class MqClientConfig {
    /**
     * @link org.apache.rocketmq.client.ClientConfig#namesrvAddr
     */
    private String nameserver;
    /**
     * @link org.apache.rocketmq.client.ClientConfig#clientCallbackExecutorThreads
     */
    private int callbackExecutorThreads = Runtime.getRuntime().availableProcessors();
    /**
     * Heartbeat interval in microseconds with message broker
     */
    private int heartbeatBrokerInterval = 1000 * 30;
    /**
     * Offset persistent interval for consumer
     */
    private int persistConsumerOffsetInterval = 1000 * 5;
    private long pullTimeDelayMillsWhenException = 1000;
    private int mqClientApiTimeout = 3 * 1000;
}
