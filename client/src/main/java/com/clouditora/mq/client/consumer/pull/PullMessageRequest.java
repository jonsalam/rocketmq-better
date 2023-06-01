package com.clouditora.mq.client.consumer.pull;

import com.clouditora.mq.client.consumer.ProcessQueue;
import com.clouditora.mq.common.topic.TopicQueue;
import lombok.Data;

/**
 * @link org.apache.rocketmq.client.impl.consumer.PullRequest
 */
@Data
public class PullMessageRequest {
    private String group;
    private TopicQueue topicQueue;
    private ProcessQueue processQueue;
    private long nextOffset;
    private boolean previouslyLocked = false;
}
