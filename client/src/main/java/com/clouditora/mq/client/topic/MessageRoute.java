package com.clouditora.mq.client.topic;

import com.clouditora.mq.common.broker.BrokerEndpoints;
import com.clouditora.mq.common.constant.GlobalConstant;
import com.clouditora.mq.common.message.MessageQueue;
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
    private List<MessageQueue> queues;
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
    public MessageQueue findOne(String lastBrokerName) {
        if (lastBrokerName == null || this.queues == null) {
            return findOne();
        }
        for (MessageQueue ignored : this.queues) {
            MessageQueue queue = findOne();
            // 切换到别的broker
            if (!lastBrokerName.equals(queue.getBrokerName())) {
                return queue;
            }
        }
        return findOne();
    }

    public MessageQueue findOne() {
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
        List<MessageQueue> messageQueues = topicRoute.getQueues().stream()
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
                            MessageQueue messageQueue = new MessageQueue();
                            messageQueue.setTopic(topic);
                            messageQueue.setBrokerName(brokerQueue.getBrokerName());
                            messageQueue.setQueueId(id);
                            return messageQueue;
                        })
                        .collect(Collectors.toList())
                )
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
        MessageRoute messageRoute = new MessageRoute();
        messageRoute.setQueues(messageQueues);
        return messageRoute;
    }
}
