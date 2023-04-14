package com.clouditora.top.nameserver.listener;

import com.clouditora.mq.network.ChannelEventListener;
import com.clouditora.top.nameserver.route.RouteInfoManager;
import io.netty.channel.Channel;

public class RouteInfoListener implements ChannelEventListener {
    private final RouteInfoManager routeInfoManager;

    public RouteInfoListener(RouteInfoManager routeInfoManager) {
        this.routeInfoManager = routeInfoManager;
    }

    @Override
    public void onConnect(String address, Channel channel) {

    }

    @Override
    public void onClose(String address, Channel channel) {
        routeInfoManager.onChannelDestroy(address, channel);
    }

    @Override
    public void onException(String address, Channel channel) {
        routeInfoManager.onChannelDestroy(address, channel);
    }

    @Override
    public void onIdle(String address, Channel channel) {
        routeInfoManager.onChannelDestroy(address, channel);
    }
}
