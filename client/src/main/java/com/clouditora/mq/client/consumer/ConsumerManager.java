package com.clouditora.mq.client.consumer;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * part of @link org.apache.rocketmq.client.impl.factory.MQClientInstance
 */
public class ConsumerManager {
    /**
     * group: consumer
     *
     * @link org.apache.rocketmq.client.impl.factory.MQClientInstance#consumerTable
     */
    private final ConcurrentMap<String, DefaultMqConsumer> consumerMap = new ConcurrentHashMap<>();

    public DefaultMqConsumer selectConsumer(String group) {
        return this.consumerMap.get(group);
    }
}
