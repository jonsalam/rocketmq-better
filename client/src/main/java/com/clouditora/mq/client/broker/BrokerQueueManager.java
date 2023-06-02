package com.clouditora.mq.client.broker;

import com.clouditora.mq.client.consumer.ProcessQueue;
import com.clouditora.mq.client.consumer.offset.AbstractOffsetManager;
import com.clouditora.mq.client.topic.TopicRouteManager;
import com.clouditora.mq.common.constant.GlobalConstant;
import com.clouditora.mq.common.constant.PositionStrategy;
import com.clouditora.mq.common.exception.BrokerException;
import com.clouditora.mq.common.exception.ClientException;
import com.clouditora.mq.common.topic.TopicQueue;
import com.clouditora.mq.network.exception.ConnectException;
import com.clouditora.mq.network.exception.TimeoutException;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @link org.apache.rocketmq.client.impl.consumer.RebalanceImpl
 * @link org.apache.rocketmq.client.impl.consumer.RebalancePushImpl
 */
@Slf4j
public class BrokerQueueManager {
    private final BrokerController brokerController;
    private final String group;
    private final String clientId;
    private final AbstractOffsetManager offsetManager;
    private final TopicRouteManager topicRouteManager;
    private final ConcurrentMap<TopicQueue, ProcessQueue> queueMessageMap = new ConcurrentHashMap<>(64);

    public BrokerQueueManager(BrokerController brokerController, String clientId, String group, AbstractOffsetManager offsetManager, TopicRouteManager topicRouteManager) {
        this.brokerController = brokerController;
        this.clientId = clientId;
        this.group = group;
        this.offsetManager = offsetManager;
        this.topicRouteManager = topicRouteManager;
    }

    public boolean lock(TopicQueue queue) {
        try {
            Set<TopicQueue> lockedQueues = this.brokerController.lockQueue(this.group, queue, this.clientId);
            for (TopicQueue lockedQueue : lockedQueues) {
                ProcessQueue processQueue = this.queueMessageMap.get(lockedQueue);
                if (processQueue != null) {
                    processQueue.setLocked(true);
                    processQueue.setLastLockTimestamp(System.currentTimeMillis());
                }
            }
            return lockedQueues.contains(queue);
        } catch (Exception e) {
            log.error("lock queue exception: {}", queue);
        }
        return false;
    }

    public void lock() {
        // 按broker name分组
        Map<String, Set<TopicQueue>> map = this.queueMessageMap.keySet().stream().collect(Collectors.groupingBy(
                TopicQueue::getBrokerName,
                Collectors.mapping(Function.identity(), Collectors.toSet())
        ));
        for (Map.Entry<String, Set<TopicQueue>> entry : map.entrySet()) {
            String brokerName = entry.getKey();
            Set<TopicQueue> queues = entry.getValue();
            try {
                Set<TopicQueue> lockedQueues = this.brokerController.lockQueue(this.group, brokerName, queues, this.clientId);
                for (TopicQueue lockedQueue : lockedQueues) {
                    ProcessQueue processQueue = this.queueMessageMap.get(lockedQueue);
                    if (processQueue != null) {
                        processQueue.setLocked(true);
                        processQueue.setLastLockTimestamp(System.currentTimeMillis());
                        log.info("lock queue success: {} {}", this.group, processQueue);
                    }
                }
                for (TopicQueue queue : queues) {
                    if (!lockedQueues.contains(queue)) {
                        ProcessQueue processQueue = this.queueMessageMap.get(queue);
                        if (processQueue != null) {
                            processQueue.setLocked(false);
                            log.info("lock queue failed: {} {}", this.group, processQueue);
                        }
                    }
                }
            } catch (Exception e) {
                log.error("lock queue exception: {}", queues);
            }
        }
    }

    /**
     * @link org.apache.rocketmq.client.impl.consumer.RebalanceImpl#unlock
     */
    public void unlock(boolean oneway, TopicQueue queue) throws BrokerException, InterruptedException, ConnectException, TimeoutException {
        this.brokerController.unlockQueue(oneway, this.group, queue, this.clientId);
    }

    public void unlock(boolean oneway) {
        // 按broker name分组
        Map<String, Set<TopicQueue>> map = this.queueMessageMap.keySet().stream().collect(Collectors.groupingBy(
                TopicQueue::getBrokerName,
                Collectors.mapping(Function.identity(), Collectors.toSet())
        ));
        for (Map.Entry<String, Set<TopicQueue>> entry : map.entrySet()) {
            String brokerName = entry.getKey();
            Set<TopicQueue> queues = entry.getValue();
            try {
                this.brokerController.unlockQueue(oneway, this.group, brokerName, queues, this.clientId);
                for (TopicQueue queue : queues) {
                    ProcessQueue processQueue = this.queueMessageMap.get(queue);
                    if (processQueue != null) {
                        processQueue.setLocked(false);
                        log.info("unlock queue failed: {} {}", this.group, processQueue);
                    }
                }
            } catch (Exception e) {
                log.error("unlock queue exception: {}", queues);
            }
        }
    }

    /**
     * @link org.apache.rocketmq.client.impl.consumer.RebalancePushImpl#computePullFromWhereWithException
     */
    public long computePullFromWhereWithException(TopicQueue topicQueue) throws ClientException {
        long result = -1;
        PositionStrategy consumeFromWhere = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().getConsumeFromWhere();
        AbstractOffsetManager offsetStore = this.defaultMQPushConsumerImpl.getOffsetStore();
        switch (consumeFromWhere) {
            case CONSUME_FROM_LAST_OFFSET -> {
                // 如果broker上没有当前group的消费进度, 从maxOffset(最新的消息)处消费, 否则接着上次的消费进度
                long lastOffset = offsetStore.get(topicQueue, ReadOffsetType.READ_FROM_STORE);
                if (lastOffset >= 0) {
                    result = lastOffset;
                } else if (lastOffset == -1) {
                    // 如果是重试队列，就从0开始消费
                    if (topicQueue.getTopic().startsWith(GlobalConstant.SystemGroup.RETRY_GROUP_TOPIC_PREFIX)) {
                        result = 0L;
                    } else {
                        try {
                            // 从maxOffset(最新的消息)处消费
                            result = this.mQClientFactory.getMQAdminImpl().maxOffset(topicQueue);
                        } catch (ClientException e) {
                            log.warn("Compute consume offset from last offset exception, mq={}, exception={}", topicQueue, e);
                            throw e;
                        }
                    }
                } else {
                    result = -1;
                }
            }
            case CONSUME_FROM_FIRST_OFFSET -> {
                // 如果broker上没有当前group的消费进度, 从0(第一条消息)处消费, 否则接着上次的消费进度
                long lastOffset = offsetStore.get(topicQueue, ReadOffsetType.READ_FROM_STORE);
                if (lastOffset >= 0) {
                    result = lastOffset;
                } else if (lastOffset == -1) {
                    result = 0L;
                } else {
                    result = -1;
                }
            }
            case CONSUME_FROM_TIMESTAMP -> {
                long lastOffset = offsetStore.get(topicQueue, ReadOffsetType.READ_FROM_STORE);
                if (lastOffset >= 0) {
                    result = lastOffset;
                } else if (lastOffset == -1) {
                    if (topicQueue.getTopic().startsWith(GlobalConstant.SystemGroup.RETRY_GROUP_TOPIC_PREFIX)) {
                        try {
                            result = this.mQClientFactory.getMQAdminImpl().maxOffset(topicQueue);
                        } catch (ClientException e) {
                            log.warn("Compute consume offset from last offset exception, mq={}, exception={}", topicQueue, e);
                            throw e;
                        }
                    } else {
                        try {
                            long timestamp = UtilAll.parseDate(this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().getConsumeTimestamp(), UtilAll.YYYYMMDDHHMMSS).getTime();
                            result = this.mQClientFactory.getMQAdminImpl().searchOffset(topicQueue, timestamp);
                        } catch (ClientException e) {
                            log.warn("Compute consume offset from last offset exception, mq={}, exception={}", topicQueue, e);
                            throw e;
                        }
                    }
                } else {
                    result = -1;
                }
            }
            default -> {
            }
        }
        return result;
    }

}
