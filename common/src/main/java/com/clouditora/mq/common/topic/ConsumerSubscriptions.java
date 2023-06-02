package com.clouditora.mq.common.topic;

import com.alibaba.fastjson2.annotation.JSONField;
import com.clouditora.mq.common.concurrent.ConsumeStrategy;
import com.clouditora.mq.common.constant.MessageModel;
import com.clouditora.mq.common.constant.PositionStrategy;
import lombok.Data;

import java.util.HashSet;
import java.util.Set;

/**
 * @link org.apache.rocketmq.common.protocol.heartbeat.ConsumerData
 */
@Data
public class ConsumerSubscriptions {
    @JSONField(name = "groupName")
    private String group;

    @JSONField(name = "consumeType")
    private ConsumeStrategy consumeStrategy;

    private MessageModel messageModel;

    @JSONField(name = "consumeFromWhere")
    private PositionStrategy positionStrategy;

    @JSONField(name = "subscriptionDataSet")
    private Set<ConsumerSubscription> subscriptions = new HashSet<>();

    public void add(ConsumerSubscription subscription) {
        this.subscriptions.add(subscription);
    }
}
