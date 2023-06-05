package com.clouditora.mq.broker.client.consumer;

import com.clouditora.mq.broker.client.TopicQueueLockEntity;
import com.clouditora.mq.common.topic.TopicQueue;
import lombok.extern.slf4j.Slf4j;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @link org.apache.rocketmq.broker.client.rebalance.RebalanceLockManager
 */
@Slf4j
public class ConsumerLockManager {
    /**
     * group:
     *
     * @link org.apache.rocketmq.broker.client.rebalance.RebalanceLockManager#mqLockTable
     */
    private final ConcurrentMap<String, ConcurrentMap<TopicQueue, TopicQueueLockEntity>> groupLockMap = new ConcurrentHashMap<>(1024);
    private final Lock lock = new ReentrantLock();

    /**
     * @link org.apache.rocketmq.broker.client.rebalance.RebalanceLockManager#tryLockBatch
     */
    public Set<TopicQueue> lockQueue(String group, Set<TopicQueue> queues, String clientId) {
        Set<TopicQueue> lockedQueues = new HashSet<>(queues.size());
        Set<TopicQueue> unlockQueues = new HashSet<>(queues.size());
        try {
            this.lock.lockInterruptibly();
            try {
                ConcurrentMap<TopicQueue, TopicQueueLockEntity> queueLockMap = this.groupLockMap.computeIfAbsent(group, e -> new ConcurrentHashMap<>());
                for (TopicQueue queue : queues) {
                    TopicQueueLockEntity lockEntity = queueLockMap.get(queue);
                    if (lockEntity != null && lockEntity.isLocked(clientId) && !lockEntity.isExpired()) {
                        lockEntity.updateTimestamp();
                        lockedQueues.add(queue);
                    } else {
                        unlockQueues.add(queue);
                    }
                }
                if (unlockQueues.isEmpty()) {
                    return lockedQueues;
                }

                for (TopicQueue queue : unlockQueues) {
                    TopicQueueLockEntity lockEntity = queueLockMap.get(queue);
                    if (lockEntity == null) {
                        queueLockMap.put(queue, new TopicQueueLockEntity(clientId));
                        lockedQueues.add(queue);
                        log.info("lock queue by {}: {}", clientId, queue);
                        continue;
                    }
                    if (lockEntity.isExpired()) {
                        queueLockMap.put(queue, new TopicQueueLockEntity(clientId));
                        lockedQueues.add(queue);
                        log.info("{} lock queue expired: {}, now locked by {}", lockEntity.getClientId(), queue, clientId);
                        continue;
                    }
                    log.info("lock queue failed by {}: queue {} already locked by {}", lockEntity.getClientId(), queue, clientId);
                }
            } finally {
                this.lock.unlock();
            }
        } catch (InterruptedException ignored) {

        }
        return lockedQueues;
    }

    /**
     * @link org.apache.rocketmq.broker.client.rebalance.RebalanceLockManager#unlockBatch
     */
    public void unlockQueue(String group, Set<TopicQueue> queues, String clientId) {
        try {
            this.lock.lockInterruptibly();
            try {
                ConcurrentMap<TopicQueue, TopicQueueLockEntity> queueLockMap = this.groupLockMap.get(group);
                if (queueLockMap == null) {
                    log.info("unlock queue: group not exists {}", group);
                    return;
                }
                for (TopicQueue queue : queues) {
                    TopicQueueLockEntity lockEntity = queueLockMap.get(queue);
                    if (lockEntity == null) {
                        log.info("unlock queue: not locked: {}", queue);
                        continue;
                    }
                    if(lockEntity.isLocked(clientId)){
                        queueLockMap.remove(queue);
                        log.info("unlock queue: {}", clientId);
                    }
                }
            } finally {
                this.lock.unlock();
            }
        } catch (InterruptedException ignored) {
        }
    }
}
