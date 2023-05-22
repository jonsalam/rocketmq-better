package com.clouditora.mq.common.message;

import com.clouditora.mq.common.constant.SendStatus;
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

    private String offsetMsgId;
}
