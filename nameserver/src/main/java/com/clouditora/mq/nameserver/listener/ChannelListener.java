package com.clouditora.mq.nameserver.listener;

import com.clouditora.mq.nameserver.route.RouteInfoManager;
import com.clouditora.mq.network.ChannelEventListener;
import io.netty.channel.Channel;

public class ChannelListener implements ChannelEventListener {
    private final RouteInfoManager routeInfoManager;

    public ChannelListener(RouteInfoManager routeInfoManager) {
        this.routeInfoManager = routeInfoManager;
    }

    @Override
    public void onConnect(String address, Channel channel) {

    }

    @Override
    public void onClose(String address, Channel channel) {
        routeInfoManager.unregisterBroker(address, channel);
    }

    @Override
    public void onException(String address, Channel channel) {
        routeInfoManager.unregisterBroker(address, channel);
    }

    @Override
    public void onIdle(String address, Channel channel) {
        routeInfoManager.unregisterBroker(address, channel);
    }
}
