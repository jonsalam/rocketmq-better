package com.clouditora.mq.network;

import com.clouditora.mq.network.client.ClientChannelHolder;
import com.clouditora.mq.network.client.ClientCommandInvoker;
import com.clouditora.mq.network.client.ClientNameServerManager;
import com.clouditora.mq.network.client.ClientNettyChannelHandler;
import com.clouditora.mq.network.coord.AbstractCoordinator;
import com.clouditora.mq.network.coord.NettyCommandDecoder;
import com.clouditora.mq.network.coord.NettyCommandEncoder;
import com.clouditora.mq.network.coord.NettyInboundHandler;
import com.clouditora.mq.network.exception.ConnectException;
import com.clouditora.mq.network.exception.TimeoutException;
import com.clouditora.mq.network.protocol.Command;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;

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
    private final ClientNameServerManager nameServerManager;
    private final ClientCommandInvoker commandInvoker;
    /**
     * @link org.apache.rocketmq.remoting.netty.NettyRemotingClient#callbackExecutor
     * Invoke the callback methods in this executor when process response.
     */
    @Setter
    private ExecutorService callbackExecutor;

    public Client(ClientNetworkConfig config, ChannelEventListener channelEventListener, Runnable nameServerScheduled) {
        super(config, channelEventListener);
        this.config = config;
        this.nettyBootstrap = new Bootstrap();
        this.nettyWorkerExecutor = new NioEventLoopGroup(1, (ThreadFactory) r -> new Thread(r, getServiceName() + "#NettyWorker"));
        ClientChannelHolder channelHolder = new ClientChannelHolder(config, this.nettyBootstrap);
        this.nameServerManager = new ClientNameServerManager(channelHolder, nameServerScheduled);
        this.commandInvoker = new ClientCommandInvoker(
                this.config,
                super.commandMap,
                getCallbackExecutor(),
                nameServerManager
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
                                        new ClientNettyChannelHandler(Client.this.channelEventExecutor, Client.this.commandInvoker, Client.this.nameServerManager),
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
        this.nameServerManager.startup();
        super.startup();
    }

    @Override
    public void shutdown() {
        this.nameServerManager.shutdown();
        super.shutdown();
    }

    @Override
    public ExecutorService getCallbackExecutor() {
        return this.callbackExecutor != null ? this.callbackExecutor : super.defaultExecutor;
    }

    public Command syncInvoke(String endpoint, Command request, long timeout) throws TimeoutException, InterruptedException, ConnectException {
        return commandInvoker.syncInvoke(endpoint, request, timeout);
    }

    public void asyncInvoke(String endpoint, Command request, long timeout, CommandFutureCallback callback) throws TimeoutException, ConnectException {
        commandInvoker.asyncInvoke(endpoint, request, timeout, callback);
    }

    public void onewayInvoke(String endpoint, Command request, long timeout) throws TimeoutException, ConnectException {
        commandInvoker.onewayInvoke(endpoint, request, timeout);
    }

    public List<String> getNameServerEndpoints() {
        return this.nameServerManager.getNameServerEndpoints();
    }

    public void updateNameServerEndpoints(List<String> list) {
        this.nameServerManager.updateNameServerEndpoints(list);
    }
}
