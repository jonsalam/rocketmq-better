package com.clouditora.mq.client.consumer;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

@Slf4j
public class ConsumerManager {
    @Getter
    private final ConcurrentMap<String, Consumer> consumerMap = new ConcurrentHashMap<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public Set<String> getGroups() {
        return this.consumerMap.keySet();
    }

    public Set<String> getTopics() {
        return this.consumerMap.values().stream().map(Consumer::getTopics).flatMap(Collection::stream).collect(Collectors.toSet());
    }

    public void register(Consumer consumer) {
        try {
            try {
                this.lock.writeLock().lockInterruptibly();
                Object prev = this.consumerMap.putIfAbsent(consumer.getGroup(), consumer);
                log.info("register: group={}, consumer={}, prev={}", consumer.getGroup(), consumer, prev);
            } finally {
                this.lock.writeLock().unlock();
            }
        } catch (Exception e) {
            log.error("register exception: group={}", consumer.getGroup(), e);
        }
    }

    public void unregister(String group) {
        try {
            try {
                this.lock.writeLock().lockInterruptibly();
                Object prev = this.consumerMap.remove(group);
                log.info("unregister: group={}, prev={}", group, prev);
            } finally {
                this.lock.writeLock().unlock();
            }
        } catch (Exception e) {
            log.error("unregister exception: group={}", group, e);
        }
    }
}
