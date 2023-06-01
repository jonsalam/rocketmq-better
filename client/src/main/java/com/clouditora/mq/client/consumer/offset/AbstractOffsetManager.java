package com.clouditora.mq.client.consumer.offset;

import com.clouditora.mq.common.service.AbstractFileService;
import com.clouditora.mq.common.topic.TopicQueue;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @link org.apache.rocketmq.client.consumer.store.OffsetStore
 */
public abstract class AbstractOffsetManager extends AbstractFileService {
    protected final String group;
    protected ConcurrentMap<TopicQueue, AtomicLong> offsetMap = new ConcurrentHashMap<>();

    public AbstractOffsetManager(String group, String path) {
        super(path);
        this.group = group;
    }

    /**
     * @link org.apache.rocketmq.client.consumer.store.LocalFileOffsetStore#updateOffset
     */
    public void update(TopicQueue queue, long offset) {
        AtomicLong prev = this.offsetMap.computeIfAbsent(queue, e -> new AtomicLong(offset));
        prev.set(offset);
    }

    /**
     * @link org.apache.rocketmq.client.consumer.store.LocalFileOffsetStore#readOffset
     */
    public long get(TopicQueue queue) {
        return Optional.ofNullable(this.offsetMap.get(queue)).map(AtomicLong::get).orElse(-1L);
    }
}
