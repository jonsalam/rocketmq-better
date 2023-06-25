package com.clouditora.mq.store.log;

import com.clouditora.mq.store.consume.ConsumeQueue;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

@Slf4j
public class ProduceQueueManager {
    /**
     * @link org.apache.rocketmq.store.CommitLog#topicQueueTable
     */
    private final Map<String, Long> producerQueueMap = new HashMap<>(1024);

    public void recover(ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueue>> consumeQueueMap) {
        for (Map<Integer, ConsumeQueue> maps : consumeQueueMap.values()) {
            for (ConsumeQueue consumeQueue : maps.values()) {
                String key = "%s-%s".formatted(consumeQueue.getTopic(), consumeQueue.getQueueId());
                this.producerQueueMap.put(key, consumeQueue.getMaxWriteOffset());
            }
        }
    }

    private Long computeIfAbsent(String topic, int queueId) {
        return this.producerQueueMap.computeIfAbsent("%s-%s".formatted(topic, queueId), e -> {
            log.debug("produce queue position: 0, topic={}, queueId={}", topic, queueId);
            return 0L;
        });
    }

    public Long get(String topic, int queueId) {
        return computeIfAbsent(topic, queueId);
    }

    public void increase(String topic, int queueId) {
        this.producerQueueMap.computeIfPresent("%s-%s".formatted(topic, queueId), (k, v) -> {
            log.debug("produce queue position: {}, topic={}, queueId={}", v + 1, topic, queueId);
            return v + 1;
        });
    }
}
