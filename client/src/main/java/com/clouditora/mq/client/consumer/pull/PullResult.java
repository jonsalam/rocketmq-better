package com.clouditora.mq.client.consumer.pull;

import com.clouditora.mq.common.message.MessageEntity;
import lombok.Data;

import java.util.List;

/**
 * @link org.apache.rocketmq.client.consumer.PullResult
 * @link org.apache.rocketmq.client.impl.consumer.PullResultExt
 */
@Data
public class PullResult {
    private PullStatus status;
    private Long nextBeginOffset;
    private Long minOffset;
    private Long maxOffset;
    private List<MessageEntity> messages;
    private long suggestWhichBrokerId;
    private byte[] messageBinary;
}
