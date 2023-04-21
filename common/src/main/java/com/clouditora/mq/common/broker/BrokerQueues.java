package com.clouditora.mq.common.broker;

import lombok.Getter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @link org.apache.rocketmq.common.protocol.route.QueueData
 */
@Getter
public class BrokerQueues {
    /**
     * brokerName: BrokerQueue
     */
    private final Map<String, BrokerQueue> queueMap = new HashMap<>();

    public BrokerQueue put(String brokerName, BrokerQueue brokerQueue) {
        return this.queueMap.put(brokerName, brokerQueue);
    }

    public boolean removeByBrokerName(String name) {
        this.queueMap.remove(name);
        return this.queueMap.isEmpty();
    }

    public Set<String> getBrokerNames() {
        return this.queueMap.keySet();
    }

    public List<BrokerQueue> getBrokerQueues() {
        return this.queueMap.values().stream().toList();
    }
}
