package com.clouditora.mq.store.consume;

import com.clouditora.mq.store.MessageStoreConfig;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ConsumeFileQueues {
    private final MessageStoreConfig config;
    /**
     * topic: [queue id: files]
     */
    private final ConcurrentMap<String, ConcurrentMap<Integer, ConsumeFileQueue>> map;

    public ConsumeFileQueues(MessageStoreConfig config) {
        this.config = config;
        this.map = new ConcurrentHashMap<>(32);
    }

    /**
     * @link org.apache.rocketmq.store.DefaultMessageStore#findConsumeQueue
     */
    public ConsumeFileQueue get(String topic, int queueId) {
        ConcurrentMap<Integer, ConsumeFileQueue> queueMap = this.map.computeIfAbsent(topic, e -> new ConcurrentHashMap<>(128));
        return queueMap.computeIfAbsent(queueId, e -> new ConsumeFileQueue(this.config, topic, queueId));
    }
}
