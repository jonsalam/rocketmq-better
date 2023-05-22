package com.clouditora.mq.client.topic;

import com.clouditora.mq.common.topic.TopicRoute;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Slf4j
public class TopicRouteManager {
    /**
     * topic:
     *
     * @link org.apache.rocketmq.client.impl.factory.MQClientInstance#topicRouteTable
     */
    private final ConcurrentMap<String, TopicRoute> map = new ConcurrentHashMap<>();

    public void put(String topic, TopicRoute route) {
        this.map.put(topic, route);
    }

    public TopicRoute get(String topic) {
        return this.map.get(topic);
    }
}
