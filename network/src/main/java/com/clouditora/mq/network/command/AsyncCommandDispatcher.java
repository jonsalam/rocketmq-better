package com.clouditora.mq.network.command;

import com.clouditora.mq.network.protocol.Command;
import io.netty.channel.ChannelHandlerContext;

/**
 * @link org.apache.rocketmq.remoting.netty.AsyncNettyRequestProcessor
 */
public interface AsyncCommandDispatcher extends CommandDispatcher {

    default void request(ChannelHandlerContext ctx, Command request, CommandCallback callback) throws Exception {
        Command response = request(ctx, request);
        callback.callback(response);
    }
}
