package com.clouditora.mq.client.rebalance;

import com.clouditora.mq.common.service.AbstractLoopedService;
import lombok.extern.slf4j.Slf4j;

/**
 * @link org.apache.rocketmq.client.impl.consumer.RebalanceService
 */
@Slf4j
public class RebalanceService extends AbstractLoopedService {

    @Override
    public String getServiceName() {
        return "Rebalance";
    }

    @Override
    protected void loop() throws Exception {

    }
}
