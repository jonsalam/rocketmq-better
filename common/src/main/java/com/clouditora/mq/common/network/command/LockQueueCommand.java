package com.clouditora.mq.common.network.command;

import com.alibaba.fastjson2.annotation.JSONField;
import com.clouditora.mq.common.network.CommandJsonBody;
import com.clouditora.mq.common.topic.TopicQueue;
import lombok.Data;

import java.util.Set;

public class LockQueueCommand {

    /**
     * @link org.apache.rocketmq.common.protocol.body.LockBatchRequestBody
     */
    @Data
    public static class RequestBody implements CommandJsonBody {
        @JSONField(name = "consumerGroup")
        private String group;
        @JSONField(name = "mqSet")
        private Set<TopicQueue> queues;
        private String clientId;
    }

    /**
     * @link org.apache.rocketmq.common.protocol.body.LockBatchResponseBody
     */
    @Data
    public static class ResponseBody implements CommandJsonBody {
        @JSONField(name = "lockOKMQSet")
        private Set<TopicQueue> queues;
    }
}
