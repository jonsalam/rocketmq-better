package com.clouditora.mq.common.command.protocol;

import com.alibaba.fastjson2.annotation.JSONField;
import com.clouditora.mq.common.constant.ConsumePositionStrategy;
import com.clouditora.mq.common.constant.MessageModel;
import lombok.Data;

import java.util.HashSet;
import java.util.Set;

public class ClientHeartBeat {
    /**
     * @link org.apache.rocketmq.common.protocol.heartbeat.HeartbeatData
     */
    @Data
    public static class RequestBody {
        @JSONField(name = "clientID")
        private String clientId;
        @JSONField(name = "producerDataSet")
        private Set<ProducerData> producers = new HashSet<>();
        @JSONField(name = "consumerDataSet")
        private Set<ConsumerData> consumers = new HashSet<>();
    }

    /**
     * @link org.apache.rocketmq.common.protocol.heartbeat.ProducerData
     */
    @Data
    public static class ProducerData {
        @JSONField(name = "groupName")
        private String group;
    }

    /**
     * @link org.apache.rocketmq.common.protocol.heartbeat.ConsumerData
     */
    @Data
    public static class ConsumerData {
        @JSONField(name = "groupName")
        private String group;
        private MessageModel messageModel;
        @JSONField(name = "consumeFromWhere")
        private ConsumePositionStrategy positionStrategy;
    }
}
