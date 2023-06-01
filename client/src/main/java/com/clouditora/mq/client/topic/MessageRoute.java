package com.clouditora.mq.client.topic;

import com.clouditora.mq.common.broker.BrokerEndpoints;
import com.clouditora.mq.common.constant.GlobalConstant;
import com.clouditora.mq.common.topic.TopicQueue;
import com.clouditora.mq.common.topic.TopicRoute;
import lombok.Setter;
import lombok.ToString;
import org.apache.commons.collections4.CollectionUtils;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @link org.apache.rocketmq.client.impl.producer.TopicPublishInfo
 */
@ToString(exclude = "tlIndex")
public class MessageRoute {
    @Setter
    private List<TopicQueue> queues;
    private final ThreadLocal<Integer> tlIndex = ThreadLocal.withInitial(() -> Math.abs(new Random().nextInt()));

    public boolean isEmpty() {
        return CollectionUtils.isEmpty(queues);
    }

    private int nextIndex() {
        Integer index = this.tlIndex.get();
        this.tlIndex.set(++index);
        return Math.abs(index & 0x7FFFFFFF) % this.queues.size();
    }

    /**
     * @link org.apache.rocketmq.client.impl.producer.TopicPublishInfo#selectOneMessageQueue()
     */
    public TopicQueue findOne(String lastBrokerName) {
        if (lastBrokerName == null || this.queues == null) {
            return findOne();
        }
        for (TopicQueue ignored : this.queues) {
            TopicQueue queue = findOne();
            // 切换到别的broker
            if (!lastBrokerName.equals(queue.getBrokerName())) {
                return queue;
            }
        }
        return findOne();
    }

    public TopicQueue findOne() {
        if (this.queues == null) {
            return null;
        }
        int index = nextIndex();
        return this.queues.get(index);
    }

    /**
     * @link org.apache.rocketmq.client.impl.factory.MQClientInstance#topicRouteData2TopicPublishInfo
     */
    public static MessageRoute build(String topic, TopicRoute topicRoute) {
        Map<String, BrokerEndpoints> endpointsMap = topicRoute.getBrokers().stream().collect(Collectors.toMap(BrokerEndpoints::getBrokerName, Function.identity()));
        List<TopicQueue> topicQueues = topicRoute.getQueues().stream()
                .filter(e -> {
                    BrokerEndpoints endpoints = endpointsMap.get(e.getBrokerName());
                    if (endpoints == null) {
                        return false;
                    }
                    // 没有master
                    return endpoints.getEndpointMap().containsKey(GlobalConstant.MASTER_ID);
                })
                .sorted()
                .map(brokerQueue -> IntStream.range(0, brokerQueue.getWriteNum())
                        .mapToObj(id -> {
                            TopicQueue topicQueue = new TopicQueue();
                            topicQueue.setTopic(topic);
                            topicQueue.setBrokerName(brokerQueue.getBrokerName());
                            topicQueue.setQueueId(id);
                            return topicQueue;
                        })
                        .collect(Collectors.toList())
                )
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
        MessageRoute messageRoute = new MessageRoute();
        messageRoute.setQueues(topicQueues);
        return messageRoute;
    }
}
