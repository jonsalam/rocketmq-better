package com.clouditora.mq.client.producer;

import com.clouditora.mq.common.topic.TopicQueue;
import lombok.Data;

/**
 * @link org.apache.rocketmq.client.producer.SendResult
 */
@Data
public class SendResult {
    private SendStatus status;
    private String messageId;
    private TopicQueue topicQueue;
    private long queueOffset;
    private String offsetMessageId;
}
