package com.clouditora.mq.client.broker.assignor;

import com.clouditora.mq.common.topic.TopicQueue;

import java.util.ArrayList;
import java.util.List;

/**
 * @link org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely
 */
public class TopicQueueAssignorAveragely implements TopicQueueAssignor {
    @Override
    public String name() {
        return "Averagely";
    }

    @Override
    public List<TopicQueue> assign(String group, List<TopicQueue> queues, List<String> customerIds, String clientId) {
        int index = customerIds.indexOf(clientId);
        if (index == -1) {
            return List.of();
        }
        int mod = queues.size() % customerIds.size();
        int average = queues.size() / customerIds.size();
        int extra = (index >= mod) ? 0 : 1;
        int startIndex = index * average + Math.min(index, mod);
        int endIndex = startIndex + average + extra;

        List<TopicQueue> result = new ArrayList<>();
        for (int i = startIndex; i < endIndex; i++) {
            result.add(queues.get(i % queues.size()));
        }
        return result;
    }
}
