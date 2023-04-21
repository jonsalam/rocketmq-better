package com.clouditora.mq.common.network.command;

import com.alibaba.fastjson2.annotation.JSONField;
import com.clouditora.mq.common.network.CommandHeader;
import com.clouditora.mq.common.network.CommandJsonBody;
import com.clouditora.mq.common.topic.TopicQueue;
import lombok.Data;

import java.util.concurrent.ConcurrentMap;

public class BrokerRegisterCommand {
    /**
     * @link org.apache.rocketmq.common.protocol.header.namesrv.RegisterBrokerRequestHeader
     */
    @Data
    public static class RequestHeader implements CommandHeader {
        private String clusterName;

        private String brokerName;

        @JSONField(name = "brokerAddr")
        private String brokerEndpoint;

        private Long brokerId;
    }

    /**
     * @link org.apache.rocketmq.common.protocol.body.RegisterBrokerBody
     */
    @Data
    public static class RequestBody implements CommandJsonBody {
        @JSONField(name = "topicConfigSerializeWrapper")
        private ConcurrentMap<String, TopicQueue> topicMap;
    }
}
