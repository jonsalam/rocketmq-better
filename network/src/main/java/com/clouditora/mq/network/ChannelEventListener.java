package com.clouditora.mq.network;

import io.netty.channel.Channel;

/**
 * @link org.apache.rocketmq.remoting.ChannelEventListener
 */
public interface ChannelEventListener {
    void onConnect(String address, Channel channel);

    void onClose(String address, Channel channel);

    void onException(String address, Channel channel);

    void onIdle(String address, Channel channel);
}
