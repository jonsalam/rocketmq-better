package com.clouditora.mq.nameserver.broker;

import com.clouditora.mq.nameserver.route.TopicRouteManager;
import com.clouditora.mq.network.netty.ChannelEventListener;
import io.netty.channel.Channel;

public class BrokerChannelListener implements ChannelEventListener {
    private final TopicRouteManager topicRouteManager;

    public BrokerChannelListener(TopicRouteManager topicRouteManager) {
        this.topicRouteManager = topicRouteManager;
    }

    @Override
    public void onConnect(String endpoint, Channel channel) {

    }

    @Override
    public void onClose(String endpoint, Channel channel) {
        this.topicRouteManager.unregisterBroker(endpoint, channel);
    }

    @Override
    public void onException(String endpoint, Channel channel) {
        this.topicRouteManager.unregisterBroker(endpoint, channel);
    }

    @Override
    public void onIdle(String endpoint, Channel channel) {
        this.topicRouteManager.unregisterBroker(endpoint, channel);
    }
}
