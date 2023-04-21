package com.clouditora.mq.common.command;

import lombok.Data;

public class BrokerRegister {
    /**
     * compressed 属性未加上, 默认 false
     *
     * @link org.apache.rocketmq.common.protocol.header.namesrv.RegisterBrokerRequestHeader
     */
    @Data
    public static class RequestHeader implements CommandHeader {
        private String clusterName;
        private String brokerName;
        private String brokerAddr;
        private Long brokerId;
    }
}
