package com.clouditora.mq.common.network.command;

import com.alibaba.fastjson2.annotation.JSONField;
import com.clouditora.mq.common.network.CommandJsonBody;
import com.clouditora.mq.common.topic.TopicQueue;
import lombok.Data;

import java.util.Set;

public class UnlockQueueCommand {

    /**
     * @link org.apache.rocketmq.common.protocol.body.UnlockBatchRequestBody
     */
    @Data
    public static class RequestBody implements CommandJsonBody {
        @JSONField(name = "consumerGroup")
        private String group;
        @JSONField(name = "mqSet")
        private Set<TopicQueue> queues;
        private String clientId;
    }
}
