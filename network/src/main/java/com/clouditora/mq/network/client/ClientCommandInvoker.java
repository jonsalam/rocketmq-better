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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

@Slf4j
@Getter
public class ClientCommandInvoker extends CommandInvoker {
    protected final ClientChannelHolder channelHolder;
    protected final ClientNameServerHolder nameServerHolder;

    public ClientCommandInvoker(ClientNetworkConfig config, ConcurrentHashMap<Integer, CommandFuture> commandMap, ExecutorService callbackExecutor, ClientNameServerHolder nameServerHolder) {
        super(config.getClientAsyncSemaphoreValue(), config.getClientOnewaySemaphoreValue(), commandMap, callbackExecutor);
        this.channelHolder = nameServerHolder.getChannelHolder();
        this.nameServerHolder = nameServerHolder;
    }

    private Channel getChannel(String address) {
        if (address == null) {
            return nameServerHolder.getOrCreateChannel();
        } else {
            return channelHolder.getOrCreateChannel(address);
        }
    }

    /**
     * @link org.apache.rocketmq.remoting.netty.NettyRemotingClient#invokeSync
     */
    public Command syncInvoke(String address, Command request, long timeout) throws TimeoutException, InterruptedException, ConnectException {
        Channel channel = getChannel(address);
        if (channel == null || !channel.isActive()) {
            channelHolder.closeChannel(channel);
            throw new ConnectException(address);
        }
        try {
            return super.syncInvoke(channel, request, timeout);
        } catch (SendException e) {
            channelHolder.closeChannel(channel);
            throw new ConnectException(address, e.getCause());
        }
    }

    /**
     * @link org.apache.rocketmq.remoting.netty.NettyRemotingClient#invokeAsync
     */
    public void asyncInvoke(String address, Command request, long timeout, CommandFutureCallback callback) throws TimeoutException, ConnectException {
        Channel channel = getChannel(address);
        if (channel == null || !channel.isActive()) {
            throw new ConnectException(address);
        }
        try {
            super.asyncInvoke(channel, request, timeout, callback);
        } catch (SendException e) {
            channelHolder.closeChannel(channel);
            throw new ConnectException(address, e.getCause());
        }
    }

    /**
     * @linkn org.apache.rocketmq.remoting.netty.NettyRemotingClient#invokeOneway
     */
    public void onewayInvoke(String address, Command request, long timeout) throws TimeoutException, ConnectException {
        Channel channel = getChannel(address);
        if (channel == null || !channel.isActive()) {
            throw new ConnectException(address);
        }
        try {
            super.onewayInvoke(channel, request, timeout);
        } catch (SendException e) {
            channelHolder.closeChannel(channel);
            throw new ConnectException(address, e.getCause());
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
