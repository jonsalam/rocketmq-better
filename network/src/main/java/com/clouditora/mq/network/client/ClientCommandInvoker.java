package com.clouditora.mq.network.client;

import com.clouditora.mq.network.ClientNetworkConfig;
import com.clouditora.mq.network.CommandFutureCallback;
import com.clouditora.mq.network.coord.CommandFuture;
import com.clouditora.mq.network.coord.CommandInvoker;
import com.clouditora.mq.network.exception.ConnectException;
import com.clouditora.mq.network.exception.SendException;
import com.clouditora.mq.network.exception.TimeoutException;
import com.clouditora.mq.network.protocol.Command;
import com.clouditora.mq.network.util.CoordinatorUtil;
import io.netty.channel.Channel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;

@Slf4j
@Getter
public class ClientCommandInvoker extends CommandInvoker {
    protected final ClientNameServerManager nameServerManager;

    public ClientCommandInvoker(ClientNetworkConfig config, ConcurrentMap<Integer, CommandFuture> commandMap, ExecutorService callbackExecutor, ClientNameServerManager nameServerManager) {
        super(config.getClientAsyncSemaphoreValue(), config.getClientOnewaySemaphoreValue(), commandMap, callbackExecutor);
        this.nameServerManager = nameServerManager;
    }

    private Channel getChannel(String endpoint) {
        if (endpoint == null) {
            return nameServerManager.getOrCreateChannel();
        } else {
            return nameServerManager.getOrCreateChannel(endpoint);
        }
    }

    /**
     * @link org.apache.rocketmq.remoting.netty.NettyRemotingClient#invokeSync
     */
    public Command syncInvoke(String endpoint, Command request, long timeout) throws TimeoutException, InterruptedException, ConnectException {
        Channel channel = getChannel(endpoint);
        if (channel == null || !channel.isActive()) {
            nameServerManager.closeChannel(channel);
            throw new ConnectException(endpoint);
        }
        try {
            return super.syncInvoke(channel, request, timeout);
        } catch (SendException e) {
            nameServerManager.closeChannel(channel);
            throw new ConnectException(endpoint, e.getCause());
        }
    }

    /**
     * @link org.apache.rocketmq.remoting.netty.NettyRemotingClient#invokeAsync
     */
    public void asyncInvoke(String endpoint, Command request, long timeout, CommandFutureCallback callback) throws TimeoutException, ConnectException {
        Channel channel = getChannel(endpoint);
        if (channel == null || !channel.isActive()) {
            throw new ConnectException(endpoint);
        }
        try {
            super.asyncInvoke(channel, request, timeout, callback);
        } catch (SendException e) {
            nameServerManager.closeChannel(channel);
            throw new ConnectException(endpoint, e.getCause());
        }
    }

    /**
     * @link org.apache.rocketmq.remoting.netty.NettyRemotingClient#invokeOneway
     */
    public void onewayInvoke(String endpoint, Command request, long timeout) throws TimeoutException, ConnectException {
        Channel channel = getChannel(endpoint);
        if (channel == null || !channel.isActive()) {
            throw new ConnectException(endpoint);
        }
        try {
            super.onewayInvoke(channel, request, timeout);
        } catch (SendException e) {
            nameServerManager.closeChannel(channel);
            throw new ConnectException(endpoint, e.getCause());
        }
    }

    /**
     * make the request of the specified channel as fail and to invoke fail callback immediately
     *
     * @param channel the channel which is close already
     * @link org.apache.rocketmq.remoting.netty.NettyRemotingAbstract#failFast
     */
    public void failFast(Channel channel) {
        super.commandMap.values().stream()
                .filter(e -> e.getChannel() == channel)
                .findFirst()
                .ifPresent(e -> failFast(e.getOpaque()));
    }

    private void failFast(int opaque) {
        CommandFuture commandFuture = super.commandMap.remove(opaque);
        if (commandFuture != null) {
            commandFuture.setSendOk(false);
            commandFuture.putResponse(null);
            CoordinatorUtil.invokeCallback(commandFuture, getCallbackExecutor());
        }
    }
}
