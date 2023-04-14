package com.clouditora.mq.client.consumer;

import lombok.Data;

/**
 * @link org.apache.rocketmq.client.impl.consumer.PullRequest
 */
@Data
public class PullRequest {
    private String consumerGroup;
    private ProcessQueue processQueue;
}
