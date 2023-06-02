package com.clouditora.mq.client.broker.allocate;

import com.clouditora.mq.common.topic.TopicQueue;

import java.util.List;

/**
 * @link org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy
 */
public interface AllocateTopicQueueStrategy {
    String name();

    List<TopicQueue> allocate(String group, List<TopicQueue> queues, List<String> customerIds, String clientId);
}
