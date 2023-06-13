package com.clouditora.mq.store.consume;

import com.clouditora.mq.store.StoreConfig;
import org.apache.commons.lang3.math.NumberUtils;

import java.io.File;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ConsumeQueueManager {
    private final StoreConfig storeConfig;
    /**
     * topic: [queue id: files]
     *
     * @link org.apache.rocketmq.store.DefaultMessageStore#consumeQueueTable
     */
    private final ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueue>> map;

    public ConsumeQueueManager(StoreConfig storeConfig) {
        this.storeConfig = storeConfig;
        this.map = new ConcurrentHashMap<>(32);
    }

    /**
     * @link org.apache.rocketmq.store.DefaultMessageStore#putConsumeQueue
     */
    public void put(String topic, int queueId, ConsumeQueue consumeQueue) {
        ConcurrentMap<Integer, ConsumeQueue> queueMap = this.map.computeIfAbsent(topic, e -> new ConcurrentHashMap<>());
        queueMap.computeIfAbsent(queueId, e -> consumeQueue);
    }

    /**
     * @link org.apache.rocketmq.store.DefaultMessageStore#findConsumeQueue
     */
    public ConsumeQueue get(String topic, int queueId) {
        ConcurrentMap<Integer, ConsumeQueue> queueMap = this.map.computeIfAbsent(topic, e -> new ConcurrentHashMap<>(128));
        return queueMap.computeIfAbsent(queueId, e -> new ConsumeQueue(this.storeConfig, topic, queueId));
    }

    /**
     * @link org.apache.rocketmq.store.DefaultMessageStore#recoverConsumeQueue
     */
    public long getMaxOffset() {
        long max = this.map.values().stream()
                .map(Map::values)
                .flatMap(Collection::stream)
                .mapToLong(e -> getMaxOffset())
                .max()
                .orElse(0);
        return max;
    }

    /**
     * topic
     * - queue id
     * -- consume queue
     *
     * @link org.apache.rocketmq.store.DefaultMessageStore#loadConsumeQueue
     */
    public void map() {
        File root = new File(this.storeConfig.getConsumeQueuePath());
        File[] topicDirs = root.listFiles();
        if (topicDirs == null) {
            return;
        }
        for (File topicDir : topicDirs) {
            String topic = topicDir.getName();
            File[] queueDirs = topicDir.listFiles();
            if (queueDirs == null) {
                continue;
            }
            for (File queueDir : queueDirs) {
                if (!NumberUtils.isCreatable(queueDir.getName())) {
                    continue;
                }
                int queueId = Integer.parseInt(queueDir.getName());
                ConsumeQueue queue = new ConsumeQueue(this.storeConfig, queueDir);
                queue.map();
                put(topic, queueId, queue);
            }
        }
    }

    /**
     * @link org.apache.rocketmq.store.ConsumeQueue#recover
     */
    public void recover(ConsumeQueue consumeQueue) {
        List<ConsumeFile> files = consumeQueue.getFiles();

    }
}
