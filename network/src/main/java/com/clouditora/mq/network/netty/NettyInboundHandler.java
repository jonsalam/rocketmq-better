package com.clouditora.mq.network.netty;

import com.clouditora.mq.network.protocol.Command;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

/**
 * @link org.apache.rocketmq.remoting.netty.NettyRemotingServer.NettyServerHandler
 * @link org.apache.rocketmq.remoting.netty.NettyRemotingClient.NettyClientHandler
 */
@ChannelHandler.Sharable
public class NettyInboundHandler extends SimpleChannelInboundHandler<Command> {
    private final AbstractNetwork abstractNetwork;

    public NettyInboundHandler(AbstractNetwork abstractNetwork) {
        this.abstractNetwork = abstractNetwork;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Command command) throws Exception {
        abstractNetwork.processCommand(ctx, command);
    }
}
