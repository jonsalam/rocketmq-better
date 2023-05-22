package com.clouditora.mq.client.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

@Slf4j
public class ProducerManager {
    private final ConcurrentMap<String, Producer> producerMap = new ConcurrentHashMap<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public boolean isNotEmpty() {
        return MapUtils.isNotEmpty(this.producerMap);
    }

    public Set<String> getGroups() {
        return this.producerMap.keySet();
    }

    public List<String> getTopics() {
        return this.producerMap.values().stream().map(Producer::getTopic).distinct().collect(Collectors.toList());
    }

    public Producer get(String topic) {
        return this.producerMap.get(topic);
    }

    /**
     * @link org.apache.rocketmq.client.impl.factory.MQClientInstance#registerProducer
     */
    public void register(Producer producer) {
        try {
            try {
                this.lock.writeLock().lockInterruptibly();
                Producer prev = this.producerMap.putIfAbsent(producer.getGroup(), producer);
                log.info("register: group={}, producer={}, prev={}", producer.getGroup(), producer, prev);
            } finally {
                this.lock.writeLock().unlock();
            }
        } catch (Exception e) {
            log.error("register exception: group={}", producer.getGroup(), e);
        }
    }

    public void unregister(String group) {
        try {
            try {
                this.lock.writeLock().lockInterruptibly();
                Object prev = this.producerMap.remove(group);
                log.info("unregister: group={}, prev={}", group, prev);
            } finally {
                this.lock.writeLock().unlock();
            }
        } catch (Exception e) {
            log.error("unregister exception: group={}", group, e);
        }
    }
}
