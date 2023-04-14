package com.clouditora.mq.network;

import com.clouditora.mq.network.protocol.Command;
import io.netty.channel.ChannelHandlerContext;

/**
 * @link org.apache.rocketmq.remoting.netty.NettyRequestProcessor
 */
public interface CommandRequestProcessor {
    Command process(ChannelHandlerContext context, Command request) throws Exception;
}
