package com.clouditora.mq.client.consumer.offset;

import com.clouditora.mq.common.message.MessageQueue;
import com.clouditora.mq.common.service.AbstractFileService;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @link org.apache.rocketmq.client.consumer.store.OffsetStore
 */
public abstract class AbstractOffsetManager extends AbstractFileService {
    protected final String group;
    protected ConcurrentMap<MessageQueue, AtomicLong> offsetMap = new ConcurrentHashMap<>();

    public AbstractOffsetManager(String group, String path) {
        super(path);
        this.group = group;
    }

    /**
     * @link org.apache.rocketmq.client.consumer.store.LocalFileOffsetStore#updateOffset
     */
    public void update(MessageQueue queue, long offset) {
        AtomicLong prev = this.offsetMap.computeIfAbsent(queue, e -> new AtomicLong(offset));
        prev.set(offset);
    }

    /**
     * @link org.apache.rocketmq.client.consumer.store.LocalFileOffsetStore#readOffset
     */
    public long get(MessageQueue queue) {
        return Optional.ofNullable(this.offsetMap.get(queue)).map(AtomicLong::get).orElse(-1L);
    }
}
