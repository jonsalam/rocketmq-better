package com.clouditora.mq.client.consumer.handler;

import com.clouditora.mq.client.consumer.listener.ConcurrentMessageListener;

/**
 * @link org.apache.rocketmq.client.impl.consumer.ConsumeMessageConcurrentlyService.ConsumeRequest
 */
public class ConsumeTask implements Runnable{
    private final ConcurrentMessageListener messageListener;

    public ConsumeTask(ConcurrentMessageListener messageListener) {
        this.messageListener = messageListener;
    }

    @Override
    public void run() {

    }
}
