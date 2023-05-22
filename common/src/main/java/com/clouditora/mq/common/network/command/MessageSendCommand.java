package com.clouditora.mq.common.network.command;

import com.alibaba.fastjson2.annotation.JSONField;
import com.clouditora.mq.common.network.CommandHeader;
import com.clouditora.mq.common.network.CommandJsonBody;
import com.clouditora.mq.common.topic.TopicQueue;
import lombok.Data;

import java.util.concurrent.ConcurrentMap;

public class MessageSendCommand {
    /**
     * @link org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader
     * @link org.apache.rocketmq.common.protocol.header.SendMessageRequestHeaderV2
     */
    @Data
    public static class RequestHeader implements CommandHeader {
        @JSONField(name = "producerGroup")
        private String group;
        private String topic;
        private Integer queueId;
        private Long bornTimestamp;
        private Integer flag;
        private String properties;
    }

    /**
     * @link org.apache.rocketmq.common.protocol.body.RegisterBrokerBody
     */
    @Data
    public static class RequestBody implements CommandJsonBody {
        @JSONField(name = "topicConfigSerializeWrapper")
        private ConcurrentMap<String, TopicQueue> topicMap;
    }

    /**
     * @link org.apache.rocketmq.common.protocol.header.SendMessageResponseHeader
     */
    @Data
    public static class ResponseHeader implements CommandHeader {
        private String msgId;
        private Integer queueId;
        private Long queueOffset;
    }
}
