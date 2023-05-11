package com.clouditora.mq.client.topic;

import com.clouditora.mq.common.route.TopicRoute;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class TopicRouteManager {
    private final ConcurrentMap<String, TopicRoute> topicRouteMap = new ConcurrentHashMap<>();

    public void addTopicRoute(String topic, TopicRoute route) {
        this.topicRouteMap.put(topic, route);
    }

    public TopicRoute getTopicRoute(String topic) {
        return this.topicRouteMap.get(topic);
    }
}
