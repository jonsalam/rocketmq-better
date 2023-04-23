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
    protected final ChannelEventExecutor channelEventExecutor;
    protected final ClientCommandInvoker commandInvoker;
    protected final ClientChannelPool channelPool;

    public ClientNettyChannelHandler(ChannelEventExecutor channelEventExecutor, ClientCommandInvoker commandInvoker, ClientChannelPool channelPool) {
        this.channelEventExecutor = channelEventExecutor;
        this.commandInvoker = commandInvoker;
        this.channelPool = channelPool;
    }

    @Override
    public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise promise) throws Exception {
        String local = CoordinatorUtil.toEndpoint(localAddress);
        String remote = CoordinatorUtil.toEndpoint(remoteAddress);
        log.debug("[channel] connect {} to {}", local, remote);
        super.connect(ctx, remoteAddress, localAddress, promise);
        channelEventExecutor.addEvent(new ChannelEvent(ChannelEventType.connect, remote, ctx.channel()));
    }

    @Override
    public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
        String endpoint = CoordinatorUtil.toEndpoint(ctx.channel());
        log.debug("[channel] disconnect {}", endpoint);
        channelPool.closeChannel(ctx.channel());
        super.disconnect(ctx, promise);
        channelEventExecutor.addEvent(new ChannelEvent(ChannelEventType.close, endpoint, ctx.channel()));
    }

    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
        String endpoint = CoordinatorUtil.toEndpoint(ctx.channel());
        log.debug("[channel] close {}", endpoint);
        channelPool.closeChannel(ctx.channel());
        super.close(ctx, promise);
        commandInvoker.failFast(ctx.channel());
        channelEventExecutor.addEvent(new ChannelEvent(ChannelEventType.close, endpoint, ctx.channel()));
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent event) {
            if (event.state() == IdleState.ALL_IDLE) {
                String endpoint = CoordinatorUtil.toEndpoint(ctx.channel());
                log.warn("[channel] idle on {}", endpoint);
                channelPool.closeChannel(ctx.channel());
                channelEventExecutor.addEvent(new ChannelEvent(ChannelEventType.idle, endpoint, ctx.channel()));
            }
        }
        ctx.fireUserEventTriggered(evt);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        String endpoint = CoordinatorUtil.toEndpoint(ctx.channel());
        log.error("[channel] exception on {}", endpoint, cause);
        channelPool.closeChannel(ctx.channel());
        channelEventExecutor.addEvent(new ChannelEvent(ChannelEventType.exception, endpoint, ctx.channel()));
    }
}
