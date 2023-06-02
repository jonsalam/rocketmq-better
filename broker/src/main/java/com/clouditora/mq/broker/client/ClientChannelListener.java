package com.clouditora.mq.broker.client;

import com.clouditora.mq.broker.BrokerController;
import com.clouditora.mq.network.netty.ChannelEventListener;
import io.netty.channel.Channel;

/**
 * @link org.apache.rocketmq.broker.client.ClientHousekeepingService
 */
public class ClientChannelListener implements ChannelEventListener {
    private final BrokerController brokerController;

    public ClientChannelListener(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    @Override
    public void onConnect(String endpoint, Channel channel) {

    }

    @Override
    public void onClose(String endpoint, Channel channel) {
        brokerController.unregisterClient(channel);
    }

    @Override
    public void onException(String endpoint, Channel channel) {
        brokerController.unregisterClient(channel);
    }

    @Override
    public void onIdle(String endpoint, Channel channel) {
        brokerController.unregisterClient(channel);
    }
}
