package com.clouditora.mq.common.network.command;

import com.alibaba.fastjson2.annotation.JSONField;
import com.clouditora.mq.common.network.CommandHeader;
import com.clouditora.mq.common.network.CommandJsonBody;
import com.clouditora.mq.common.topic.TopicQueueConfig;
import lombok.Data;
import lombok.EqualsAndHashCode;

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
        private String defaultTopic;
        @JSONField(name = "defaultTopicQueueNums")
        private Integer defaultTopicQueueNum;
        private Integer queueId;
        private Integer sysFlag;
        private Long bornTimestamp;
        private Integer flag;
        private String properties;
        @JSONField(name = "reconsumeTimes")
        private Integer reConsumeTimes;
        @JSONField(name = "maxReconsumeTimes")
        private Integer maxReConsumeTimes;
    }

    @EqualsAndHashCode(callSuper = true)
    @Data
    public static class RequestHeaderV2 extends RequestHeader {
        @JSONField(name = "a")
        private String group;
        @JSONField(name = "b")
        private String topic;
        @JSONField(name = "c")
        private String defaultTopic;
        @JSONField(name = "d")
        private Integer defaultTopicQueueNum;
        @JSONField(name = "e")
        private Integer queueId;
        @JSONField(name = "f")
        private Integer sysFlag;
        @JSONField(name = "g")
        private Long bornTimestamp;
        @JSONField(name = "h")
        private Integer flag;
        @JSONField(name = "i")
        private String properties;
        @JSONField(name = "j")
        private Integer reConsumeTimes;
        @JSONField(name = "m")
        private Integer maxReConsumeTimes;
    }

    /**
     * @link org.apache.rocketmq.common.protocol.body.RegisterBrokerBody
     */
    @Data
    public static class RequestBody implements CommandJsonBody {
        @JSONField(name = "topicConfigSerializeWrapper")
        private ConcurrentMap<String, TopicQueueConfig> topicMap;
    }

    /**
     * @link org.apache.rocketmq.common.protocol.header.SendMessageResponseHeader
     */
    @Data
    public static class ResponseHeader implements CommandHeader {
        @JSONField(name = "msgId")
        private String messageId;
        private Integer queueId;
        private Long queueOffset;
    }
}
