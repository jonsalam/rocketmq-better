package com.clouditora.mq.broker.listener;

import com.clouditora.mq.network.ChannelEventListener;
import io.netty.channel.Channel;

public class BrokerChannelListener implements ChannelEventListener {
    @Override
    public void onConnect(String endpoint, Channel channel) {

    }

    @Override
    public void onClose(String endpoint, Channel channel) {

    }

    @Override
    public void onException(String endpoint, Channel channel) {

    }

    @Override
    public void onIdle(String endpoint, Channel channel) {

    }
}
