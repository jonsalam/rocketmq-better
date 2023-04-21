package com.clouditora.mq.network;

import io.netty.channel.Channel;

/**
 * @link org.apache.rocketmq.remoting.ChannelEventListener
 */
public interface ChannelEventListener {
    void onConnect(String endpoint, Channel channel);

    void onClose(String endpoint, Channel channel);

    void onException(String endpoint, Channel channel);

    void onIdle(String endpoint, Channel channel);
}
