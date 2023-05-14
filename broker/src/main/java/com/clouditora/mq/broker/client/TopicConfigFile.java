package com.clouditora.mq.broker.client;

import com.clouditora.mq.common.topic.TopicConfig;
import lombok.Data;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Data
public class TopicConfigFile {
    private ConcurrentMap<String, TopicConfig> topicConfigTable = new ConcurrentHashMap<String, TopicConfig>();
}
