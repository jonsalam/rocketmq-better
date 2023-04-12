package com.clouditora.mq.network.command;

import com.clouditora.mq.network.protocol.Command;
import io.netty.channel.ChannelHandlerContext;

/**
 * @link org.apache.rocketmq.remoting.netty.NettyRequestProcessor
 */
public interface CommandDispatcher {
    Command request(ChannelHandlerContext context, Command request) throws Exception;
}
