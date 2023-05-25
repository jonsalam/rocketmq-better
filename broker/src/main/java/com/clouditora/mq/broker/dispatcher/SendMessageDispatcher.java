package com.clouditora.mq.broker.dispatcher;

import com.clouditora.mq.network.command.CommandDispatcher;
import com.clouditora.mq.network.protocol.Command;
import io.netty.channel.ChannelHandlerContext;

public class SendMessageDispatcher implements CommandDispatcher {
    @Override
    public Command request(ChannelHandlerContext context, Command request) throws Exception {
        return null;
    }
}
