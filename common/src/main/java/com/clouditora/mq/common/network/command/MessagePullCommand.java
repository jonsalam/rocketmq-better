package com.clouditora.mq.common.network.command;

import com.alibaba.fastjson2.annotation.JSONField;
import com.clouditora.mq.common.network.CommandHeader;
import lombok.Data;

public class MessagePullCommand {
    /**
     * @link org.apache.rocketmq.common.protocol.header.PullMessageRequestHeader
     */
    @Data
    public static class RequestHeader implements CommandHeader {
        @JSONField(name = "consumerGroup")
        private String group;
        private String topic;
        private Integer queueId;
        private Long queueOffset;
        @JSONField(name = "maxMsgNums")
        private Integer maxNumber;
        private Integer sysFlag;
        private Long commitOffset;
        @JSONField(name = "suspendTimeoutMillis")
        private Long suspendTimeout;
        private String subscription;
        private Long subVersion;
        private String expressionType;
    }

    @Data
    public static class ResponseHeader implements CommandHeader {
        private Long suggestWhichBrokerId;
        private Long nextBeginOffset;
        private Long minOffset;
        private Long maxOffset;
    }
}
