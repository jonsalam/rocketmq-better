package com.clouditora.mq.common.topic;

import com.alibaba.fastjson2.annotation.JSONField;
import lombok.Data;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @link org.apache.rocketmq.common.protocol.body.TopicConfigSerializeWrapper
 */
@Data
public class TopicQueueFile {
    @JSONField(name = "topicConfigTable")
    private ConcurrentMap<String, TopicQueueConfig> topicMap = new ConcurrentHashMap<>();
}
