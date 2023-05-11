package com.clouditora.mq.client.consumer;

import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Slf4j
public class ConsumerManager {
    private final ConcurrentMap<String, Consumer> consumerMap = new ConcurrentHashMap<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public ConsumerManager() {
    }

    public List<String> getTopics() {
        return null;
    }

    public void register(String group, Consumer consumer) {
        try {
            try {
                lock.writeLock().lockInterruptibly();
                Object prev = this.consumerMap.putIfAbsent(group, consumer);
                log.info("register: group={}, consumer={}, prev={}", group, consumer, prev);
            } finally {
                this.lock.writeLock().unlock();
            }
        } catch (Exception e) {
            log.error("register exception: group={}", group, e);
        }
    }

    public void unregister(String group) {
        try {
            try {
                lock.writeLock().lockInterruptibly();
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
