package com.clouditora.mq.client.broker.assignor;

import com.clouditora.mq.common.topic.TopicQueue;

import java.util.List;

/**
 * @link org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy
 */
public interface TopicQueueAssignor {
    String name();

    List<TopicQueue> assign(String group, List<TopicQueue> queues, List<String> customerIds, String clientId);
}
