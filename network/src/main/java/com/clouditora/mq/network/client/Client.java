package com.clouditora.mq.network.client;

import com.clouditora.mq.common.util.ThreadUtil;
import com.clouditora.mq.network.ChannelEventListener;
import com.clouditora.mq.network.ClientNetworkConfig;
import com.clouditora.mq.network.CommandFutureCallback;
import com.clouditora.mq.network.coord.AbstractCoordinator;
import com.clouditora.mq.network.coord.NettyCommandDecoder;
import com.clouditora.mq.network.coord.NettyCommandEncoder;
import com.clouditora.mq.network.coord.NettyInboundHandler;
import com.clouditora.mq.network.exception.ConnectException;
import com.clouditora.mq.network.exception.TimeoutException;
import com.clouditora.mq.network.protocol.Command;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;

@Slf4j
@Getter
public class Client extends AbstractCoordinator {
    private final ClientNetworkConfig config;
    /**
     * @link org.apache.rocketmq.remoting.netty.NettyRemotingClient#nettyClientConfig
     */
    private final Bootstrap nettyBootstrap;
    /**
     * @link org.apache.rocketmq.remoting.netty.NettyRemotingClient#eventLoopGroupWorker
     */
    private final EventLoopGroup nettyWorkerExecutor;
    private final ClientChannelHolder channelHolder;
    private final ClientNameServerHolder nameServerHolder;
    private final ClientCommandInvoker commandInvoker;
    private final ConcurrentMap<String, ChannelFuture> channelFutureMap = new ConcurrentHashMap<>();
    /**
     * @link org.apache.rocketmq.remoting.netty.NettyRemotingClient#callbackExecutor
     * Invoke the callback methods in this executor when process response.
     */
    @Setter
    private ExecutorService callbackExecutor;

    public Client(ClientNetworkConfig config, ChannelEventListener channelEventListener) {
        super(config, channelEventListener);
        this.config = config;
        this.nettyBootstrap = new Bootstrap();
        this.nettyWorkerExecutor = new NioEventLoopGroup(1, ThreadUtil.buildFactory(getServiceName() + ":NettyWorker", 1));
        this.channelHolder = new ClientChannelHolder(config, this.nettyBootstrap, channelFutureMap);
        this.nameServerHolder = new ClientNameServerHolder(channelHolder);
        this.commandInvoker = new ClientCommandInvoker(
                this.config,
                super.commandMap,
                getCallbackExecutor(),
                nameServerHolder
        );
    }

    @Override
    public String getServiceName() {
        return "Client";
    }

    @Override
    public void startup() {
        this.nettyBootstrap.group(this.nettyWorkerExecutor)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, false)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, config.getConnectTimeoutMillis())
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel channel) throws Exception {
                        channel.pipeline()
                                .addLast(
                                        Client.this.nettyDefaultEventExecutor,
                                        new NettyCommandEncoder(),
                                        new NettyCommandDecoder(),
                                        new IdleStateHandler(0, 0, config.getClientChannelMaxIdleTimeSeconds()),
                                        new ClientNettyChannelHandler(Client.this.channelEventExecutor, Client.this.commandInvoker),
                                        new NettyInboundHandler(Client.this)
                                );
                    }
                });
        if (config.getClientSocketSndBufSize() > 0) {
            log.info("[server] set SO_SNDBUF to {}", config.getClientSocketSndBufSize());
            this.nettyBootstrap.option(ChannelOption.SO_SNDBUF, config.getClientSocketSndBufSize());
        }
        if (config.getClientSocketRcvBufSize() > 0) {
            log.info("[server] set SO_RCVBUF to {}", config.getClientSocketRcvBufSize());
            this.nettyBootstrap.option(ChannelOption.SO_RCVBUF, config.getClientSocketRcvBufSize());
        }
        if (config.getWriteBufferLowWaterMark() > 0 && config.getWriteBufferHighWaterMark() > 0) {
            log.info("[server] set WRITE_BUFFER_WATER_MARK to {}, {}", config.getWriteBufferLowWaterMark(), config.getWriteBufferHighWaterMark());
            WriteBufferWaterMark waterMark = new WriteBufferWaterMark(config.getWriteBufferLowWaterMark(), config.getWriteBufferHighWaterMark());
            this.nettyBootstrap.option(ChannelOption.WRITE_BUFFER_WATER_MARK, waterMark);
        }
        super.startup();
    }

    @Override
    public void shutdown() {
        super.shutdown();
    }

    @Override
    public ExecutorService getCallbackExecutor() {
        return this.callbackExecutor != null ? this.callbackExecutor : super.defaultExecutor;
    }

    public Command syncInvoke(String address, Command request, long timeout) throws TimeoutException, InterruptedException, ConnectException {
        return commandInvoker.syncInvoke(address, request, timeout);
    }

    public void asyncInvoke(String address, Command request, long timeout, CommandFutureCallback callback) throws TimeoutException, ConnectException {
        commandInvoker.asyncInvoke(address, request, timeout, callback);
    }

    public void onewayInvoke(String address, Command request, long timeout) throws TimeoutException, ConnectException {
        commandInvoker.onewayInvoke(address, request, timeout);
    }

}
