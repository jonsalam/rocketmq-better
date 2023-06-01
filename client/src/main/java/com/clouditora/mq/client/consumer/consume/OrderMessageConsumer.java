package com.clouditora.mq.client.consumer.consume;

import com.clouditora.mq.common.service.AbstractScheduledService;

/**
 * @link org.apache.rocketmq.client.impl.consumer.ConsumeMessageOrderlyService
 */
public class OrderMessageConsumer extends AbstractScheduledService implements AbstractMessageConsumer {
    @Override
    public String getServiceName() {
        return "OrderMessageConsume";
    }


    /**
     * @link org.apache.rocketmq.client.impl.consumer.ConsumeMessageOrderlyService#lockMQPeriodically
     */
    private void lockBroker() {

    }

    /**
     * @link org.apache.rocketmq.client.impl.consumer.ConsumeMessageOrderlyService#unlockAllMQ
     */
    private void unlockBroker() {

    }
}
