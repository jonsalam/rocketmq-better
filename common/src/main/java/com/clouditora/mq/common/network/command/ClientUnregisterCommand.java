package com.clouditora.mq.common.network.command;

import com.alibaba.fastjson2.annotation.JSONField;
import com.clouditora.mq.common.network.CommandHeader;
import lombok.Data;

public class ClientUnregisterCommand {
    /**
     * @link org.apache.rocketmq.common.protocol.header.UnregisterClientRequestHeader
     */
    @Data
    public static class RequestHeader implements CommandHeader {
        @JSONField(name = "clientID")
        private String clientId;

        private String producerGroup;

        private String consumerGroup;
    }
}
