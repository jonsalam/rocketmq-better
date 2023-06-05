package com.clouditora.mq.broker.client.consumer;

import com.clouditora.mq.common.concurrent.ConsumeStrategy;
import com.clouditora.mq.common.constant.MessageModel;
import com.clouditora.mq.common.constant.PositionStrategy;
import com.clouditora.mq.common.topic.GroupSubscription;
import com.clouditora.mq.common.topic.TopicSubscription;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * @link org.apache.rocketmq.broker.client.ConsumerGroupInfo
 */
@Slf4j
@ToString
public class ConsumerSubscribeManager {
    private final String group;
    /**
     * topic:
     *
     * @link org.apache.rocketmq.broker.client.ConsumerGroupInfo#subscriptionTable
     */
    private final ConcurrentMap<String, TopicSubscription> subscriptionMap = new ConcurrentHashMap<>();
    /**
     * @link org.apache.rocketmq.broker.client.ConsumerGroupInfo#consumeType
     */
    private volatile ConsumeStrategy consumeStrategy = ConsumeStrategy.PUSH;
    @Getter
    private volatile MessageModel messageModel;
    /**
     * @link org.apache.rocketmq.broker.client.ConsumerGroupInfo#consumeFromWhere
     */
    private volatile PositionStrategy positionStrategy;
    private volatile long updateTime = System.currentTimeMillis();

    public ConsumerSubscribeManager(String group, MessageModel messageModel, PositionStrategy positionStrategy) {
        this.group = group;
        this.messageModel = messageModel;
        this.positionStrategy = positionStrategy;
    }

    /**
     * @link org.apache.rocketmq.broker.client.ConsumerGroupInfo#updateChannel
     * @link org.apache.rocketmq.broker.client.ConsumerGroupInfo#updateSubscription
     */
    public void update(GroupSubscription groupSubscription) {
        // 更新订阅
        {
            for (TopicSubscription subscription : groupSubscription.getSubscriptions()) {
                TopicSubscription prev = this.subscriptionMap.computeIfAbsent(subscription.getTopic(), e -> {
                    log.info("register consumer {}: {}", subscription.getTopic(), subscription);
                    return subscription;
                });
                if (subscription.getVersion() > prev.getVersion()) {
                    log.info("change consumer {}: {}", prev, subscription);
                }
            }
        }
        // 移除无用订阅
        {
            Set<String> topics = groupSubscription.getSubscriptions().stream().map(TopicSubscription::getTopic).collect(Collectors.toSet());
            Iterator<Map.Entry<String, TopicSubscription>> iterator = this.subscriptionMap.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, TopicSubscription> next = iterator.next();
                String topic = next.getKey();
                TopicSubscription subscription = next.getValue();
                if (!topics.contains(topic)) {
                    iterator.remove();
                    log.info("unregister consumer {}: {}", subscription.getTopic(), subscription);
                }
            }
        }
        this.messageModel = groupSubscription.getMessageModel();
        this.positionStrategy = groupSubscription.getPositionStrategy();
        this.updateTime = System.currentTimeMillis();
    }

    public TopicSubscription get(String topic) {
        return this.subscriptionMap.get(topic);
    }
}
