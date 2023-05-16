package com.clouditora.mq.common.topic;

import com.alibaba.fastjson2.annotation.JSONField;
import lombok.Data;

import java.util.Set;

/**
 * @link org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData
 */
@Data
public class ConsumerSubscription {
    public final static String SUB_ALL = "*";

    private String topic;

    @JSONField(name = "subString")
    private String expression;

    @JSONField(name = "tagsSet")
    private Set<String> tags;

    @JSONField(name = "codeSet")
    private Set<Integer> codes;

    @JSONField(name = "subVersion")
    private long version = System.currentTimeMillis();
}
