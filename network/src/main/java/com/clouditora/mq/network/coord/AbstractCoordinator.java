package com.clouditora.mq.network.coord;

import com.clouditora.mq.common.command.RequestCode;
import com.clouditora.mq.common.service.AbstractNothingService;
import com.clouditora.mq.common.util.ThreadUtil;
import com.clouditora.mq.network.ChannelEventListener;
import com.clouditora.mq.network.CommandRequestProcessor;
import com.clouditora.mq.network.protocol.Command;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;

/**
 * @link org.apache.rocketmq.remoting.netty.NettyRemotingAbstract
 */
@Slf4j
@Getter
public abstract class AbstractCoordinator extends AbstractNothingService implements CallbackExecutor {
    /**
     * This map caches all processing requests.
     * key: opaque
     *
     * @link org.apache.rocketmq.remoting.netty.NettyRemotingAbstract#responseTable
     */
    protected final ConcurrentMap<Integer, CommandFuture> commandMap = new ConcurrentHashMap<>(256);
    protected final CommandCleaner commandCleaner;
    protected final NettyCommandHandler nettyCommandHandler;
    /**
     * Executor to feed netty events to user defined {@link ChannelEventListener}.
     */
    protected final ChannelEventExecutor channelEventExecutor;
    /**
     * @link org.apache.rocketmq.remoting.netty.NettyRemotingServer#defaultEventExecutorGroup
     */
    protected final DefaultEventExecutorGroup nettyDefaultEventExecutor;
    /**
     * @link org.apache.rocketmq.remoting.netty.NettyRemotingServer#publicExecutor
     */
    protected final ExecutorService defaultExecutor;

    protected AbstractCoordinator(CoordinatorConfig config, ChannelEventListener channelEventListener) {
        this.commandCleaner = new CommandCleaner(commandMap, getCallbackExecutor());
        this.nettyCommandHandler = new NettyCommandHandler(commandMap, getCallbackExecutor());

        this.channelEventExecutor = new ChannelEventExecutor(channelEventListener);
        this.nettyDefaultEventExecutor = new DefaultEventExecutorGroup(
                config.getWorkerThreads(),
                ThreadUtil.buildFactory(getServiceName() + "#NettyDefault", config.getWorkerThreads())
        );

        int publicNum = Math.max(config.getCallbackExecutorThreads(), 4);
        this.defaultExecutor = ThreadUtil.newFixedThreadPool(publicNum, getServiceName() + "#Default");
    }

    @Override
    public void startup() {
        this.commandCleaner.startup();
        this.channelEventExecutor.startup();
    }

    @Override
    public void shutdown() {
        this.channelEventExecutor.shutdown();
        log.info("{} shutdown: channelEventExecutor", getServiceName());
        this.commandCleaner.shutdown();
        log.info("{} shutdown: commandCleaner", getServiceName());
        this.defaultExecutor.shutdown();
        log.info("{} shutdown: defaultExecutor", getServiceName());
        this.nettyDefaultEventExecutor.shutdownGracefully();
        log.info("{} shutdown: nettyDefaultEventExecutor", getServiceName());
    }

    public void registerProcessor(int code, CommandRequestProcessor processor, ExecutorService executor) {
        this.nettyCommandHandler.registerProcessor(code, processor, executor);
    }

    public void registerProcessor(RequestCode code, CommandRequestProcessor processor, ExecutorService executor) {
        registerProcessor(code.getCode(), processor, executor);
    }

    public void setDefaultProcessor(CommandRequestProcessor processor, ExecutorService executor) {
        this.nettyCommandHandler.setDefaultProcessor(CommandRequestExecutor.of(processor, executor));
    }

    public void processCommand(ChannelHandlerContext channel, Command command) throws Exception {
        nettyCommandHandler.processCommand(channel, command);
    }

}
