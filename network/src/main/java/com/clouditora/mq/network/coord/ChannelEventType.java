package com.clouditora.mq.network.coord;

/**
 * @link org.apache.rocketmq.remoting.netty.NettyEventType
 */
public enum ChannelEventType {
    connect,
    close,
    idle,
    exception
}
