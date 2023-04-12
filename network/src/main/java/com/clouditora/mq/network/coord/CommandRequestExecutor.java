package com.clouditora.mq.network.coord;

import com.clouditora.mq.network.CommandRequestProcessor;
import com.clouditora.mq.network.CommandRequestProcessorAsync;
import com.clouditora.mq.network.protocol.Command;
import com.clouditora.mq.network.protocol.ResponseCode;
import com.clouditora.mq.network.util.CoordinatorUtil;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

@Slf4j
public class CommandRequestExecutor {
    private CommandRequestProcessor processor;
    private ExecutorService executorService;

    public static CommandRequestExecutor of(CommandRequestProcessor processor, ExecutorService executorService) {
        CommandRequestExecutor executor = new CommandRequestExecutor();
        executor.processor = processor;
        executor.executorService = executorService;
        return executor;
    }

    /**
     * @link org.apache.rocketmq.remoting.netty.NettyRemotingAbstract#processRequestCommand
     */
    public void processRequestCommand(ChannelHandlerContext context, Command request) {
        try {
            this.executorService.submit(() -> {
                try {
                    if (this.processor instanceof CommandRequestProcessorAsync async) {
                        async.process(context, request, (response) -> callback(context, request, response));
                    } else {
                        Command response = this.processor.process(context, request);
                        callback(context, request, response);
                    }
                } catch (Exception e) {
                    log.error("[command][request] process exception: command={}", request, e);
                    Command response = Command.buildResponse(ResponseCode.SYSTEM_ERROR, CoordinatorUtil.simplifyException(e));
                    response.setOpaque(request.getOpaque());
                    context.writeAndFlush(response);
                }
            });
        } catch (RejectedExecutionException e) {
            log.warn("[command][request] process reject: command={}, executorService={}", request, this.executorService, e);
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
