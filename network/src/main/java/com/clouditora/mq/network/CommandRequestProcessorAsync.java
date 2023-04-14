package com.clouditora.mq.network;

import com.clouditora.mq.network.protocol.Command;
import io.netty.channel.ChannelHandlerContext;

/**
 * @link org.apache.rocketmq.remoting.netty.AsyncNettyRequestProcessor
 */
public interface CommandRequestProcessorAsync extends CommandRequestProcessor {

    default void process(ChannelHandlerContext ctx, Command request, CommandCallback callback) throws Exception {
        Command response = process(ctx, request);
        callback.callback(response);
    }
}
