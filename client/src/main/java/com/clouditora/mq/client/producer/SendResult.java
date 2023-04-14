package com.clouditora.mq.client.producer;

import com.clouditora.mq.common.message.MessageQueue;
import lombok.Data;

/**
 * @link org.apache.rocketmq.client.producer.SendResult
 */
@Data
public class SendResult {
    private SendStatus sendStatus;
    private String msgId;
    private MessageQueue messageQueue;
    private long queueOffset;
    private String transactionId;
    private String offsetMsgId;
    private String regionId;
    private boolean traceOn = true;
}
