package com.clouditora.mq.common.network.command;

import com.alibaba.fastjson2.annotation.JSONField;
import com.clouditora.mq.common.network.CommandJsonBody;
import com.clouditora.mq.common.topic.ConsumerSubscriptions;
import com.clouditora.mq.common.topic.ProducerGroup;
import lombok.Data;

import java.util.Set;

public class ClientRegisterCommand {
    /**
     * @link org.apache.rocketmq.common.protocol.heartbeat.HeartbeatData
     */
    @Data
    public static class RequestBody implements CommandJsonBody {
        @JSONField(name = "clientID")
        private String clientId;

        @JSONField(name = "producerDataSet")
        private Set<ProducerGroup> producers;

        @JSONField(name = "consumerDataSet")
        private Set<ConsumerSubscriptions> consumers;
    }

}
