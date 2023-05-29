package com.clouditora.mq.store.index;

import com.clouditora.mq.store.MessageStoreConfig;

import java.io.File;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ConsumeFileMap {
    private final MessageStoreConfig config;
    private final ConcurrentMap<String, ConcurrentMap<Integer, ConsumeFileQueue>> consumeMap;

    public ConsumeFileMap(MessageStoreConfig config) {
        this.config = config;
        this.consumeMap = new ConcurrentHashMap<>(32);
    }

    public ConsumeFileQueue findConsumeQueue(String topic, int queueId) {
        ConcurrentMap<Integer, ConsumeFileQueue> queueMap = this.consumeMap.computeIfAbsent(topic, e -> new ConcurrentHashMap<>(128));
        return queueMap.computeIfAbsent(queueId, e -> {
            String path = this.config.getConsumeQueuePath() + File.separator + topic + File.separator + queueId;
            return new ConsumeFileQueue(path, this.config.getConsumeQueueFileSize());
        });
    }
}
