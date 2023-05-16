package com.clouditora.mq.broker.client;

import com.clouditora.mq.network.netty.ChannelEventListener;
import io.netty.channel.Channel;

/**
 * @link org.apache.rocketmq.broker.client.ClientHousekeepingService
 */
public class ClientChannelListener implements ChannelEventListener {
    private final ProducerManager producerManager;
    private final ConsumerManager consumerManager;

    public ClientChannelListener(ProducerManager producerManager, ConsumerManager consumerManager) {
        this.producerManager = producerManager;
        this.consumerManager = consumerManager;
    }

    @Override
    public void onConnect(String endpoint, Channel channel) {

    }

    @Override
    public void onClose(String endpoint, Channel channel) {
        producerManager.unregister(channel);
        consumerManager.unregister(channel);
    }

    @Override
    public void onException(String endpoint, Channel channel) {
        producerManager.unregister(channel);
        consumerManager.unregister(channel);
    }

    @Override
    public void onIdle(String endpoint, Channel channel) {
        producerManager.unregister(channel);
        consumerManager.unregister(channel);
    }
}
