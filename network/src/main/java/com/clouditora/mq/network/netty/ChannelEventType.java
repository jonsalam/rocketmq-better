package com.clouditora.mq.network.netty;

/**
 * @link org.apache.rocketmq.remoting.netty.NettyEventType
 */
public enum ChannelEventType {
    connect,
    close,
    idle,
    exception
}
