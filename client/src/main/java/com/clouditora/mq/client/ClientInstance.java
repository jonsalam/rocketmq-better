package com.clouditora.mq.client;

import com.clouditora.mq.client.rebalance.RebalanceService;
import com.clouditora.mq.client.consumer.PullMessageService;

/**
 * @link org.apache.rocketmq.client.impl.factory.MQClientInstance
 */
public class ClientInstance {
    private final PullMessageService pullMessageService;
    private final RebalanceService rebalanceService;

    public ClientInstance(PullMessageService pullMessageService, RebalanceService rebalanceService) {
        this.pullMessageService = pullMessageService;
        this.rebalanceService = rebalanceService;
    }
}
