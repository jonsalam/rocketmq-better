package com.clouditora.mq.network.coord;

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
    private final AbstractCoordinator coordinator;

    public NettyInboundHandler(AbstractCoordinator coordinator) {
        this.coordinator = coordinator;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Command command) throws Exception {
        coordinator.processCommand(ctx, command);
    }
}
