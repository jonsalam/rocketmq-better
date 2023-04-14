package com.clouditora.mq.network.client;

import com.clouditora.mq.network.coord.ChannelEvent;
import com.clouditora.mq.network.coord.ChannelEventExecutor;
import com.clouditora.mq.network.coord.ChannelEventType;
import com.clouditora.mq.network.util.CoordinatorUtil;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import lombok.extern.slf4j.Slf4j;

import java.net.SocketAddress;

/**
 * @link org.apache.rocketmq.remoting.netty.NettyRemotingClient.NettyConnectManageHandler
 */
@ChannelHandler.Sharable
@Slf4j
public class ClientNettyChannelHandler extends ChannelDuplexHandler {
    private final ChannelEventExecutor channelEventExecutor;
    private final ClientChannelHolder channelHolder;
    private final ClientCommandInvoker commandInvoker;

    public ClientNettyChannelHandler(ChannelEventExecutor channelEventExecutor, ClientCommandInvoker commandInvoker) {
        this.channelEventExecutor = channelEventExecutor;
        this.channelHolder = commandInvoker.getChannelHolder();
        this.commandInvoker = commandInvoker;
    }

    @Override
    public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise promise) throws Exception {
        String local = CoordinatorUtil.toAddress(localAddress);
        String remote = CoordinatorUtil.toAddress(remoteAddress);
        log.debug("[channel] connect {} to {}", local, remote);
        super.connect(ctx, remoteAddress, localAddress, promise);
        channelEventExecutor.addEvent(new ChannelEvent(ChannelEventType.connect, remote, ctx.channel()));
    }

    @Override
    public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
        String address = CoordinatorUtil.toAddress(ctx.channel());
        log.debug("[channel] disconnect {}", address);
        channelHolder.closeChannel(ctx.channel());
        super.disconnect(ctx, promise);
        channelEventExecutor.addEvent(new ChannelEvent(ChannelEventType.close, address, ctx.channel()));
    }

    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
        String address = CoordinatorUtil.toAddress(ctx.channel());
        log.debug("[channel] close {}", address);
        channelHolder.closeChannel(ctx.channel());
        super.close(ctx, promise);
        commandInvoker.failFast(ctx.channel());
        channelEventExecutor.addEvent(new ChannelEvent(ChannelEventType.close, address, ctx.channel()));
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent event) {
            if (event.state() == IdleState.ALL_IDLE) {
                String address = CoordinatorUtil.toAddress(ctx.channel());
                log.warn("[channel] idle on {}", address);
                channelHolder.closeChannel(ctx.channel());
                channelEventExecutor.addEvent(new ChannelEvent(ChannelEventType.idle, address, ctx.channel()));
            }
        }
        ctx.fireUserEventTriggered(evt);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        String address = CoordinatorUtil.toAddress(ctx.channel());
        log.error("[channel] exception on {}", address, cause);
        channelHolder.closeChannel(ctx.channel());
        channelEventExecutor.addEvent(new ChannelEvent(ChannelEventType.exception, address, ctx.channel()));
    }
}
