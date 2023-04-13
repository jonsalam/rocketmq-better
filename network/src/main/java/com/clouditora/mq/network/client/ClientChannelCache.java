package com.clouditora.mq.network.client;

import com.clouditora.mq.network.ClientNetworkConfig;
import com.clouditora.mq.network.util.NetworkUtil;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.net.SocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

@Slf4j
@Getter
class ClientChannelCache {
    /**
     * @link org.apache.rocketmq.remoting.netty.NettyRemotingClient#LOCK_TIMEOUT_MILLIS
     */
    static final long LOCK_TIMEOUT_MILLIS = 3000;

    protected final ClientNetworkConfig config;
    protected final Function<SocketAddress, ChannelFuture> connect;
    /**
     * @link org.apache.rocketmq.remoting.netty.NettyRemotingClient#channelTables
     */
    protected final ConcurrentMap<String, ChannelFuture> channelFutureMap = new ConcurrentHashMap<>();
    protected final Lock lock = new ReentrantLock();

    public ClientChannelCache(ClientNetworkConfig config, Function<SocketAddress, ChannelFuture> connect) {
        this.config = config;
        this.connect = connect;
    }

    /**
     * @link org.apache.rocketmq.remoting.netty.NettyRemotingClient#closeChannel(io.netty.channel.Channel)
     */
    public void closeChannel(Channel channel) {
        if (channel == null) {
            return;
        }
        try {
            String endpoint = NetworkUtil.toEndpoint(channel);
            if (!this.lock.tryLock(3000, TimeUnit.MILLISECONDS)) {
                log.error("close channel {} wait timeout", endpoint);
                return;
            }
            try {
                ChannelFuture channelFuture = this.channelFutureMap.get(endpoint);
                if (channelFuture != null && channelFuture.channel() == channel) {
                    this.channelFutureMap.remove(endpoint);
                    NetworkUtil.closeChannel(channel);
                }
            } catch (Exception e) {
                log.error("close channel {} exception", NetworkUtil.toEndpoint(channel), e);
            } finally {
                this.lock.unlock();
            }
        } catch (InterruptedException e) {
            log.error("close channel {} exception", NetworkUtil.toEndpoint(channel), e);
        }
    }

    /**
     * @link NettyRemotingClient#channelTables.get(addr)
     */
    public Channel getChannel(String endpoint) {
        ChannelFuture channelFuture = this.channelFutureMap.get(endpoint);
        if (NetworkUtil.isActive(channelFuture)) {
            return channelFuture.channel();
        }
        return null;
    }

    /**
     * @link org.apache.rocketmq.remoting.netty.NettyRemotingClient#createChannel
     */
    public Channel createChannel(String endpoint) {
        Channel channel = getChannel(endpoint);
        if (channel != null) {
            return channel;
        }
        try {
            if (!this.lock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                log.error("create channel {} wait timeout", endpoint);
                return null;
            }
            try {
                ChannelFuture channelFuture = this.channelFutureMap.get(endpoint);
                if (channelFuture == null) {
                    return connectToEndpoint(endpoint);
                }
                if (NetworkUtil.isActive(channelFuture)) {
                    return channelFuture.channel();
                }
                if (!channelFuture.isDone()) {
                    return awaitChannelFuture(channelFuture);
                }
                this.channelFutureMap.remove(endpoint);
                return connectToEndpoint(endpoint);
            } catch (Exception e) {
                log.error("create channel {} exception", endpoint, e);
            } finally {
                this.lock.unlock();
            }
        } catch (InterruptedException e) {
            log.error("create channel {} exception", endpoint, e);
        }
        return null;
    }

    private Channel connectToEndpoint(String endpoint) {
        ChannelFuture channelFuture = this.channelFutureMap.computeIfAbsent(endpoint, e -> connect.apply(NetworkUtil.toSocketAddress(e)));
        if (channelFuture != null) {
            return awaitChannelFuture(channelFuture);
        }
        return null;
    }

    private Channel awaitChannelFuture(ChannelFuture channelFuture) {
        if (channelFuture.awaitUninterruptibly(this.config.getConnectTimeoutMillis())) {
            if (NetworkUtil.isActive(channelFuture)) {
                log.info("create channel {} success", channelFuture);
                return channelFuture.channel();
            } else {
                log.error("create channel {} failed", channelFuture);
            }
        } else {
            log.error("create channel {} timeout", channelFuture);
        }
        return null;
    }
}
