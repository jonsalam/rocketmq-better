package com.clouditora.mq.client.consumer.handler;

import com.clouditora.mq.client.consumer.ConsumerConfig;
import com.clouditora.mq.common.service.AbstractScheduledService;
import com.clouditora.mq.common.util.ThreadUtil;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @link org.apache.rocketmq.client.impl.consumer.ConsumeMessageService
 */
public abstract class AbstractMessageHandler extends AbstractScheduledService {
    protected final ConsumerConfig config;
    /**
     * @link org.apache.rocketmq.client.impl.consumer.ConsumeMessageOrderlyService#consumeExecutor
     */
    private final ThreadPoolExecutor consumeExecutor;

    public AbstractMessageHandler(ConsumerConfig config) {
        this.config = config;
        this.consumeExecutor = new ThreadPoolExecutor(
                config.getConsumeThreadMin(),
                config.getConsumeThreadMax(),
                1000 * 60,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                ThreadUtil.buildFactory("%s#%s".formatted(getServiceName(), config.getConsumerGroup()), config.getConsumeThreadMax())
        );
    }

    @Override
    public void startup() {
        super.startup();
    }

    @Override
    public void shutdown() {
        this.consumeExecutor.shutdown();
        super.shutdown();
    }

}
