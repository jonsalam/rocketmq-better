package com.clouditora.mq.common.topic;

import lombok.Data;

import java.util.Comparator;

/**
 * @link org.apache.rocketmq.common.message.MessageQueue
 */
@Data
public class TopicQueue implements Comparable<TopicQueue> {
    private String brokerName;
    private String topic;
    private int queueId;

    /**
     * @link org.apache.rocketmq.common.message.MessageQueue#compareTo
     */
    @Override
    public int compareTo(TopicQueue topicQueue) {
        return Comparator
                .comparing(TopicQueue::getTopic)
                .thenComparing(TopicQueue::getBrokerName)
                .thenComparing(TopicQueue::getQueueId)
                .compare(this, topicQueue);
    }
}
