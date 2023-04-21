package com.clouditora.mq.common.network.command;

import com.alibaba.fastjson2.annotation.JSONField;
import com.clouditora.mq.common.network.CommandHeader;
import lombok.Data;

public class BrokerUnregisterCommand {
    /**
     * @link org.apache.rocketmq.common.protocol.header.namesrv.UnRegisterBrokerRequestHeader
     */
    @Data
    public static class RequestHeader implements CommandHeader {
        private String clusterName;

        private String brokerName;

        private Long brokerId;

        @JSONField(name = "brokerAddr")
        private String brokerEndpoint;
    }
}
