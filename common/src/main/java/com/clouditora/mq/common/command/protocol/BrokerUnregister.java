package com.clouditora.mq.common.command.protocol;

import com.alibaba.fastjson2.annotation.JSONField;
import com.clouditora.mq.common.command.CommandHeader;
import lombok.Data;

public class BrokerUnregister {
    /**
     * @link org.apache.rocketmq.common.protocol.header.namesrv.UnRegisterBrokerRequestHeader
     */
    @Data
    public static class RequestHeader implements CommandHeader {
        private String clusterName;
        private String brokerName;
        @JSONField(name = "brokerAddr")
        private String brokerEndpoint;
        private Long brokerId;
    }
}
