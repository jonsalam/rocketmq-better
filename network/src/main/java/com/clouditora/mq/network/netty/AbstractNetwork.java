package com.clouditora.mq.network.netty;

import com.clouditora.mq.common.network.RequestCode;
import com.clouditora.mq.common.service.AbstractNothingService;
import com.clouditora.mq.common.util.ThreadUtil;
import com.clouditora.mq.network.command.CommandCleaner;
import com.clouditora.mq.network.command.CommandDispatcher;
import com.clouditora.mq.network.command.CommandDispatcherExecutor;
import com.clouditora.mq.network.command.CommandFuture;
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
public abstract class AbstractNetwork extends AbstractNothingService implements CallbackExecutor {
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

    protected AbstractNetwork(NetworkConfig config, ChannelEventListener channelEventListener) {
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
        this.commandCleaner.shutdown();
        this.defaultExecutor.shutdown();
        this.nettyDefaultEventExecutor.shutdownGracefully();
    }

    public void registerDispatcher(int code, CommandDispatcher dispatcher, ExecutorService executor) {
        this.nettyCommandHandler.registerDispatcher(code, dispatcher, executor);
    }

    public void registerDispatcher(RequestCode code, CommandDispatcher dispatcher, ExecutorService executor) {
        registerDispatcher(code.getCode(), dispatcher, executor);
    }

    public void setDefaultDispatcher(CommandDispatcher dispatcher, ExecutorService executor) {
        this.nettyCommandHandler.setDefaultDispatcher(CommandDispatcherExecutor.of(dispatcher, executor));
    }

    public void processCommand(ChannelHandlerContext channel, Command command) throws Exception {
        nettyCommandHandler.dispatchCommand(channel, command);
    }

}
