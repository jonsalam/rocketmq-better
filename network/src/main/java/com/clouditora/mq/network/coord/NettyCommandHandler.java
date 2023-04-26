package com.clouditora.mq.network.coord;

import com.clouditora.mq.network.CommandRequestProcessor;
import com.clouditora.mq.network.protocol.Command;
import com.clouditora.mq.network.protocol.CommandType;
import com.clouditora.mq.network.protocol.ResponseCode;
import com.clouditora.mq.network.util.CoordinatorUtil;
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
    protected final Map<Integer, CommandRequestExecutor> processorMap = new HashMap<>(64);
    /**
     * The default request processor to use in case there is no exact match in {@link #processorMap} per request code.
     *
     * @link org.apache.rocketmq.remoting.netty.NettyRemotingAbstract#defaultRequestProcessor
     */
    @Setter
    protected CommandRequestExecutor defaultProcessor;
    protected ExecutorService callbackExecutor;

    public NettyCommandHandler(ConcurrentMap<Integer, CommandFuture> commandMap, ExecutorService callbackExecutor) {
        this.commandMap = commandMap;
        this.callbackExecutor = callbackExecutor;
    }

    public void registerProcessor(int code, CommandRequestProcessor processor, ExecutorService executor) {
        if (executor == null) {
            return;
        }
        this.processorMap.put(code, CommandRequestExecutor.of(processor, executor));
    }

    public void processCommand(ChannelHandlerContext context, Command command) {
        log.debug("process: {}", command);
        if (command == null) {
            return;
        }
        if (command.getType() == CommandType.REQUEST) {
            processRequestCommand(context, command);
        } else if (command.getType() == CommandType.RESPONSE) {
            processResponseCommand(context, command);
        }
    }

    /**
     * @link org.apache.rocketmq.remoting.netty.NettyRemotingAbstract#processRequestCommand
     */
    public void processRequestCommand(ChannelHandlerContext context, Command command) {
        CommandRequestExecutor executor = Optional
                .ofNullable(this.processorMap.get(command.getCode()))
                .orElse(this.defaultProcessor);

        if (executor == null) {
            String msg = String.format("request code [%s] is not supported", command.getCode());
            Command response = Command.buildResponse(ResponseCode.NOT_SUPPORTED, msg);
            response.setOpaque(command.getOpaque());
            context.writeAndFlush(response);
            log.error("{} request code [{}] is not supported", CoordinatorUtil.toEndpoint(context.channel()), command.getCode());
            return;
        }

        executor.processRequestCommand(context, command);
    }

    /**
     * @link org.apache.rocketmq.remoting.netty.NettyRemotingAbstract#processResponseCommand
     */
    public void processResponseCommand(ChannelHandlerContext context, Command command) {
        int opaque = command.getOpaque();
        CommandFuture commandFuture = this.commandMap.get(opaque);
        if (commandFuture == null) {
            log.warn("receive request from {}/{}, but not matched any request", CoordinatorUtil.toEndpoint(context.channel()), opaque);
            return;
        }
        this.commandMap.remove(opaque);

        if (commandFuture.getCallback() == null) {
            commandFuture.putResponse(command);
        } else {
            CoordinatorUtil.invokeCallback(commandFuture, getCallbackExecutor());
        }
    }
}
