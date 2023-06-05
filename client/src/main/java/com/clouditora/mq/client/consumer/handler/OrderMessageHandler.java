package com.clouditora.mq.client.consumer.handler;

import com.clouditora.mq.client.consumer.ConsumerConfig;

/**
 * @link org.apache.rocketmq.client.impl.consumer.ConsumeMessageOrderlyService
 */
public class OrderMessageHandler extends AbstractMessageHandler {
    public OrderMessageHandler(ConsumerConfig config) {
        super(config);
    }

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
