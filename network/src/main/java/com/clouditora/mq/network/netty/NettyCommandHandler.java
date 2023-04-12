package com.clouditora.mq.network.netty;

import com.clouditora.mq.network.command.CommandDispatcher;
import com.clouditora.mq.network.command.CommandDispatcherExecutor;
import com.clouditora.mq.network.command.CommandFuture;
import com.clouditora.mq.network.protocol.Command;
import com.clouditora.mq.network.protocol.CommandType;
import com.clouditora.mq.network.protocol.ResponseCode;
import com.clouditora.mq.network.util.NetworkUtil;
import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;

@Slf4j
@Getter
public class NettyCommandHandler implements CallbackExecutor {
    protected final ConcurrentMap<Integer, CommandFuture> commandMap;
    /**
     * This container holds all processors per request code, aka, for each incoming request, we may look up the
     * responding processor in this map to handle the request.
     * key: request code
     *
     * @link org.apache.rocketmq.remoting.netty.NettyRemotingAbstract#processorTable
     */
    protected final Map<Integer, CommandDispatcherExecutor> dispatcherMap = new HashMap<>(64);
    /**
     * The default request processor to use in case there is no exact match in {@link #dispatcherMap} per request code.
     *
     * @link org.apache.rocketmq.remoting.netty.NettyRemotingAbstract#defaultRequestProcessor
     */
    @Setter
    protected CommandDispatcherExecutor defaultDispatcher;
    protected ExecutorService callbackExecutor;

    public NettyCommandHandler(ConcurrentMap<Integer, CommandFuture> commandMap, ExecutorService callbackExecutor) {
        this.commandMap = commandMap;
        this.callbackExecutor = callbackExecutor;
    }

    public void registerDispatcher(int code, CommandDispatcher dispatcher, ExecutorService executor) {
        if (executor == null) {
            return;
        }
        this.dispatcherMap.put(code, CommandDispatcherExecutor.of(dispatcher, executor));
    }

    public void dispatchCommand(ChannelHandlerContext context, Command command) {
        log.debug("process: {}", command);
        if (command == null) {
            return;
        }
        if (command.getType() == CommandType.REQUEST) {
            dispatchRequestCommand(context, command);
        } else if (command.getType() == CommandType.RESPONSE) {
            dispatchResponseCommand(context, command);
        }
    }

    /**
     * @link org.apache.rocketmq.remoting.netty.NettyRemotingAbstract#processRequestCommand
     */
    public void dispatchRequestCommand(ChannelHandlerContext context, Command request) {
        CommandDispatcherExecutor dispatcher = Optional
                .ofNullable(this.dispatcherMap.get(request.getCode()))
                .orElse(this.defaultDispatcher);

        if (dispatcher != null) {
            dispatcher.request(context, request);
            return;
        }
        String msg = String.format("request code [%s] is not supported", request.getCode());
        Command response = Command.buildResponse(ResponseCode.NOT_SUPPORTED, msg);
        response.setOpaque(request.getOpaque());
        context.writeAndFlush(response);
        log.error("{} request code [{}] is not supported", NetworkUtil.toEndpoint(context.channel()), request.getCode());
    }

    /**
     * @link org.apache.rocketmq.remoting.netty.NettyRemotingAbstract#processResponseCommand
     */
    public void dispatchResponseCommand(ChannelHandlerContext context, Command response) {
        int opaque = response.getOpaque();
        CommandFuture commandFuture = this.commandMap.get(opaque);
        if (commandFuture == null) {
            log.warn("receive request from {}/{}, but not matched any request", NetworkUtil.toEndpoint(context.channel()), opaque);
            return;
        }
        this.commandMap.remove(opaque);

        if (commandFuture.getCallback() == null) {
            commandFuture.putResponse(response);
        } else {
            NetworkUtil.invokeCallback(commandFuture, getCallbackExecutor());
        }
    }
}
