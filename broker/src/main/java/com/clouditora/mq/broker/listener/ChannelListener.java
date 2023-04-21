package com.clouditora.mq.broker.listener;

import com.clouditora.mq.network.ChannelEventListener;
import io.netty.channel.Channel;

public class ChannelListener implements ChannelEventListener {
    @Override
    public void onConnect(String address, Channel channel) {

    }

    @Override
    public void onClose(String address, Channel channel) {

    }

    @Override
    public void onException(String address, Channel channel) {

    }

    @Override
    public void onIdle(String address, Channel channel) {

    }
}
