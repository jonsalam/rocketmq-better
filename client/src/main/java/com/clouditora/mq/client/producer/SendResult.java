package com.clouditora.mq.client.producer;

import com.clouditora.mq.common.message.MessageQueue;
import lombok.Data;

/**
 * @link org.apache.rocketmq.client.producer.SendResult
 */
@Data
public class SendResult {
    private SendStatus status;
    private String messageId;
    private MessageQueue messageQueue;
    private long queueOffset;
    private String offsetMessageId;
}
