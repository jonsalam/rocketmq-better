package com.clouditora.mq.network.coord;

import io.netty.channel.Channel;
import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @link org.apache.rocketmq.remoting.netty.NettyEvent
 */
@Data
@AllArgsConstructor
public class ChannelEvent {
    private final ChannelEventType type;
    private final String address;
    private final Channel channel;
}
