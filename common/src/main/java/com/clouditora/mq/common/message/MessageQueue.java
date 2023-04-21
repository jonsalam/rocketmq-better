package com.clouditora.mq.common.message;

import lombok.Data;

import java.util.Comparator;

/**
 * @link org.apache.rocketmq.common.message.MessageQueue
 */
@Data
public class MessageQueue implements Comparable<MessageQueue> {
    private String topic;
    private String brokerName;
    private int queueId;

    @Override
    public int compareTo(MessageQueue messageQueue) {
        return Comparator
                .comparing(MessageQueue::getTopic)
                .thenComparing(MessageQueue::getBrokerName)
                .thenComparing(MessageQueue::getQueueId)
                .compare(this, messageQueue);
    }
}
