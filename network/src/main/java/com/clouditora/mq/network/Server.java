package com.clouditora.mq.network;

import com.clouditora.mq.common.util.ThreadUtil;
import com.clouditora.mq.network.coord.*;
import com.clouditora.mq.network.exception.SendException;
import com.clouditora.mq.network.exception.TimeoutException;
import com.clouditora.mq.network.protocol.Command;
import com.clouditora.mq.network.server.ServerNettyChannelHandler;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;

/**
 * @link org.apache.rocketmq.remoting.netty.NettyRemotingServer
 */
@Slf4j
public class Server extends AbstractCoordinator {
    private final ServerNetworkConfig config;
    private final ServerBootstrap nettyServerBootstrap;
    /**
     * @link org.apache.rocketmq.remoting.netty.NettyRemotingServer#eventLoopGroupBoss
     */
    private final EventLoopGroup nettySelectExecutor;
    /**
     * @link org.apache.rocketmq.remoting.netty.NettyRemotingServer#eventLoopGroupSelector
     */
    private final EventLoopGroup nettyWorkExecutor;
    private final CommandInvoker commandInvoker;

    public Server(ServerNetworkConfig config, ChannelEventListener channelEventListener) {
        super(config, channelEventListener);
        this.config = config;
        this.nettyServerBootstrap = new ServerBootstrap();

        int num = config.getServerSelectorThreads();
        if (useEpoll()) {
            this.nettySelectExecutor = new EpollEventLoopGroup(1, ThreadUtil.buildFactory(getServiceName() + "#NettySelector", 1));
            this.nettyWorkExecutor = new EpollEventLoopGroup(num, ThreadUtil.buildFactory(getServiceName() + "#NettyWorker", num));
        } else {
            this.nettySelectExecutor = new NioEventLoopGroup(1, ThreadUtil.buildFactory(getServiceName() + "#NettySelector", 1));
            this.nettyWorkExecutor = new NioEventLoopGroup(num, ThreadUtil.buildFactory(getServiceName() + "#NettyWorker", num));
        }

        this.commandInvoker = new CommandInvoker(
                this.config.getServerAsyncSemaphoreValue(),
                this.config.getServerOnewaySemaphoreValue(),
                super.commandMap,
                getCallbackExecutor()
        );
    }

    @Override
    public String getServiceName() {
        return "Server";
    }

    @Override
    public void startup() {
        this.nettyServerBootstrap.group(this.nettySelectExecutor, this.nettyWorkExecutor)
                .channel(useEpoll() ? EpollServerSocketChannel.class : NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, config.getServerSocketBacklog())
                .option(ChannelOption.SO_REUSEADDR, true)
                .option(ChannelOption.SO_KEEPALIVE, false)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .localAddress(new InetSocketAddress(config.getListenPort()))
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel channel) throws Exception {
                        channel.pipeline()
                                .addLast(
                                        Server.this.nettyDefaultEventExecutor,
                                        new NettyCommandEncoder(),
                                        new NettyCommandDecoder(),
                                        new IdleStateHandler(0, 0, config.getServerChannelMaxIdleTimeSeconds()),
                                        new ServerNettyChannelHandler(Server.this.channelEventExecutor),
                                        new NettyInboundHandler(Server.this)
                                );
                    }
                });
        if (config.getServerSocketSndBufSize() > 0) {
            log.info("[server] set SO_SNDBUF to {}", config.getServerSocketSndBufSize());
            this.nettyServerBootstrap.childOption(ChannelOption.SO_SNDBUF, config.getServerSocketSndBufSize());
        }
        if (config.getServerSocketRcvBufSize() > 0) {
            log.info("[server] set SO_RCVBUF to {}", config.getServerSocketRcvBufSize());
            this.nettyServerBootstrap.childOption(ChannelOption.SO_RCVBUF, config.getServerSocketRcvBufSize());
        }
        if (config.getWriteBufferLowWaterMark() > 0 && config.getWriteBufferHighWaterMark() > 0) {
            log.info("[server] set WRITE_BUFFER_WATER_MARK to {}, {}", config.getWriteBufferLowWaterMark(), config.getWriteBufferHighWaterMark());
            WriteBufferWaterMark waterMark = new WriteBufferWaterMark(config.getWriteBufferLowWaterMark(), config.getWriteBufferHighWaterMark());
            this.nettyServerBootstrap.childOption(ChannelOption.WRITE_BUFFER_WATER_MARK, waterMark);
        }
        if (config.isServerPooledByteBufAllocatorEnable()) {
            this.nettyServerBootstrap.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        }

        try {
            this.nettyServerBootstrap.bind().sync();
        } catch (InterruptedException e) {
            throw new RuntimeException("[server] start failed", e);
        }

        super.startup();
    }

    @Override
    public void shutdown() {
        try {
            this.nettySelectExecutor.shutdownGracefully();
            log.info("{} shutdown: nettySelectExecutor", getServiceName());
            this.nettyWorkExecutor.shutdownGracefully();
            log.info("{} shutdown: nettyWorkExecutor", getServiceName());
            super.shutdown();
        } catch (Exception e) {
            log.error("[server] stop exception", e);
        }
    }

    private boolean useEpoll() {
        return this.config.isUseEpollNativeSelector();
    }

    @Override
    public void registerProcessor(int code, CommandRequestProcessor processor, ExecutorService executor) {
        super.registerProcessor(code, processor, executor == null ? this.defaultExecutor : executor);
    }

    @Override
    public ExecutorService getCallbackExecutor() {
        return defaultExecutor;
    }

    public Command syncInvoke(Channel channel, Command command, long timeout) throws SendException, TimeoutException, InterruptedException {
        return commandInvoker.syncInvoke(channel, command, timeout);
    }

    public void asyncInvoke(Channel channel, Command request, long timeout, CommandFutureCallback callback) throws SendException, TimeoutException {
        commandInvoker.asyncInvoke(channel, request, timeout, callback);
    }

    public void onewayInvoke(Channel channel, Command request, long timeout) throws SendException, TimeoutException {
        commandInvoker.onewayInvoke(channel, request, timeout);
    }
}
