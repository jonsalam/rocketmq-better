package com.clouditora.mq.network.server;

import com.clouditora.mq.network.netty.ChannelEvent;
import com.clouditora.mq.network.netty.ChannelEventExecutor;
import com.clouditora.mq.network.netty.ChannelEventType;
import com.clouditora.mq.network.util.NetworkUtil;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import lombok.extern.slf4j.Slf4j;

/**
 * @link org.apache.rocketmq.remoting.netty.NettyRemotingServer.NettyConnectManageHandler
 */
@ChannelHandler.Sharable
@Slf4j
public class ServerNettyChannelHandler extends ChannelDuplexHandler {
    private final ChannelEventExecutor executor;

    public ServerNettyChannelHandler(ChannelEventExecutor executor) {
        this.executor = executor;
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        log.debug("[channel] registered on {}", NetworkUtil.toEndpoint(ctx.channel()));
        super.channelRegistered(ctx);
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        log.debug("[channel] unregistered on {}", NetworkUtil.toEndpoint(ctx.channel()));
        super.channelUnregistered(ctx);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        String endpoint = NetworkUtil.toEndpoint(ctx.channel());
        log.debug("[channel] active on {}", endpoint);
        super.channelActive(ctx);
        executor.addEvent(new ChannelEvent(ChannelEventType.connect, endpoint, ctx.channel()));
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        String endpoint = NetworkUtil.toEndpoint(ctx.channel());
        log.debug("[channel] inactive on {}", endpoint);
        super.channelInactive(ctx);
        executor.addEvent(new ChannelEvent(ChannelEventType.close, endpoint, ctx.channel()));
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent event) {
            if (event.state() == IdleState.ALL_IDLE) {
                String endpoint = NetworkUtil.toEndpoint(ctx.channel());
                log.warn("[channel] idle on {}", endpoint);
                NetworkUtil.closeChannel(ctx.channel());
                executor.addEvent(new ChannelEvent(ChannelEventType.idle, endpoint, ctx.channel()));
            }
        }
        ctx.fireUserEventTriggered(evt);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        String endpoint = NetworkUtil.toEndpoint(ctx.channel());
        log.error("[channel] exception on {}", endpoint, cause);
        executor.addEvent(new ChannelEvent(ChannelEventType.exception, endpoint, ctx.channel()));
        NetworkUtil.closeChannel(ctx.channel());
    }
}
