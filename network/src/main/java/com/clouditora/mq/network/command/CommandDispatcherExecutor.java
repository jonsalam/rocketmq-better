package com.clouditora.mq.network.command;

import com.clouditora.mq.network.protocol.Command;
import com.clouditora.mq.network.protocol.ResponseCode;
import com.clouditora.mq.network.util.NetworkUtil;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

@Slf4j
public class CommandDispatcherExecutor {
    private CommandDispatcher dispatcher;
    private ExecutorService executor;

    public static CommandDispatcherExecutor of(CommandDispatcher dispatcher, ExecutorService executor) {
        CommandDispatcherExecutor dispatcherExecutor = new CommandDispatcherExecutor();
        dispatcherExecutor.dispatcher = dispatcher;
        dispatcherExecutor.executor = executor;
        return dispatcherExecutor;
    }

    /**
     * @link org.apache.rocketmq.remoting.netty.NettyRemotingAbstract#processRequestCommand
     */
    public void request(ChannelHandlerContext context, Command request) {
        try {
            this.executor.submit(() -> {
                try {
                    if (this.dispatcher instanceof AsyncCommandDispatcher async) {
                        async.request(context, request, response -> callback(context, request, response));
                    } else {
                        Command response = this.dispatcher.request(context, request);
                        callback(context, request, response);
                    }
                } catch (Exception e) {
                    log.error("[command][request] process exception: command={}", request, e);
                    Command response = Command.buildResponse(ResponseCode.SYSTEM_ERROR, NetworkUtil.simplifyException(e));
                    response.setOpaque(request.getOpaque());
                    context.writeAndFlush(response);
                }
            });
        } catch (RejectedExecutionException e) {
            log.warn("[command][request] process reject: command={}, executorService={}", request, this.executor, e);
            Command response = Command.buildResponse(ResponseCode.SYSTEM_BUSY, "[OVERLOAD]system busy");
            response.setOpaque(request.getOpaque());
            context.writeAndFlush(response);
        }
    }

    private void callback(ChannelHandlerContext context, Command request, Command response) {
        if (request.isOneway()) {
            return;
        }
        response.setOpaque(request.getOpaque());
        response.markResponseType();
        response.setSerializeType(request.getSerializeType());
        try {
            context.writeAndFlush(response);
        } catch (Throwable e) {
            log.error("[command][request] process exception: request={}, response={}", request, response, e);
        }
    }
}
