package com.clouditora.mq.common.command;

import lombok.Data;

public class BrokerUnregister {
    /**
     * @link org.apache.rocketmq.common.protocol.header.namesrv.UnRegisterBrokerRequestHeader
     */
    @Data
    public static class RequestHeader implements CommandHeader {
        private String brokerName;
        private String brokerAddr;
        private String clusterName;
        private Long brokerId;
    }
}
