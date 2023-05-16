package com.clouditora.mq.client.topic;

import com.clouditora.mq.common.topic.TopicRoute;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class TopicRouteManager {
    private final ConcurrentMap<String, TopicRoute> map = new ConcurrentHashMap<>();

    public void addTopicRoute(String topic, TopicRoute route) {
        this.map.put(topic, route);
    }

    public TopicRoute getTopicRoute(String topic) {
        return this.map.get(topic);
    }
}
