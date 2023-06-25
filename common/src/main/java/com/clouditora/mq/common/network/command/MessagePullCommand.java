package com.clouditora.mq.common.network.command;

import com.alibaba.fastjson2.annotation.JSONField;
import com.clouditora.mq.common.constant.ExpressionType;
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

        /**
         * 拉取的消息数量
         */
        @JSONField(name = "maxMsgNums")
        private Integer pullNum;

        /**
         * 拉取的消费位点
         */
        @JSONField(name = "queueOffset")
        private Long pullOffset;

        /**
         * 上报的消费位点
         */
        private Long commitOffset;

        private Integer sysFlag;

        @JSONField(name = "suspendTimeoutMillis")
        private Long suspendTimeout;

        @JSONField(name = "subscription")
        private String expression;

        private ExpressionType expressionType;
    }

    @Data
    public static class ResponseHeader implements CommandHeader {
        private Long nextBeginOffset;

        private Long minOffset;

        private Long maxOffset;
    }
}
