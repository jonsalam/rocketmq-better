package com.clouditora.mq.client.topic;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Slf4j
public class MessageRouteManager {
    /**
     * topic:
     *
     * @link org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl#topicPublishInfoTable
     */
    private final ConcurrentMap<String, MessageRoute> map = new ConcurrentHashMap<>();

    /**
     * @link org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl#updateTopicPublishInfo
     */
    public void put(String topic, MessageRoute route) {
        this.map.put(topic, route);
    }

    /**
     * @link org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl#tryToFindTopicPublishInfo
     */
    public MessageRoute get(String topic) {
        return this.map.get(topic);
    }
}
