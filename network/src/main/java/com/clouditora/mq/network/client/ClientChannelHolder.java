package com.clouditora.mq.network.client;

import com.clouditora.mq.network.ClientNetworkConfig;
import com.clouditora.mq.network.util.CoordinatorUtil;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
@Getter
public class ClientChannelHolder {
    protected final ClientNetworkConfig config;
    protected final Bootstrap bootstrap;
    protected final ConcurrentMap<String, ChannelFuture> channelFutureMap;
    protected final Lock lock = new ReentrantLock();

    public ClientChannelHolder(ClientNetworkConfig config, Bootstrap bootstrap) {
        this.config = config;
        this.bootstrap = bootstrap;
        this.channelFutureMap = new ConcurrentHashMap<>();
    }

    /**
     * @link org.apache.rocketmq.remoting.netty.NettyRemotingClient#closeChannel(io.netty.channel.Channel)
     */
    public void closeChannel(Channel channel) {
        if (channel == null) {
            return;
        }
        try {
            String endpoint = CoordinatorUtil.toEndpoint(channel);
            if (!this.lock.tryLock(3000, TimeUnit.MILLISECONDS)) {
                log.error("close channel {} wait timeout", endpoint);
                return;
            }
            try {
                ChannelFuture channelFuture = this.channelFutureMap.get(endpoint);
                if (channelFuture != null && channelFuture.channel() == channel) {
                    this.channelFutureMap.remove(endpoint);
                    CoordinatorUtil.closeChannel(channel);
                }
            } catch (Exception e) {
                log.error("close channel {} exception", CoordinatorUtil.toEndpoint(channel), e);
            } finally {
                this.lock.unlock();
            }
        } catch (InterruptedException e) {
            log.error("close channel {} exception", CoordinatorUtil.toEndpoint(channel), e);
        }
    }

    public Channel getChannel(String endpoint) {
        ChannelFuture channelFuture = this.channelFutureMap.get(endpoint);
        if (CoordinatorUtil.isActive(channelFuture)) {
            return channelFuture.channel();
        }
        return null;
    }

    /**
     * @link org.apache.rocketmq.remoting.netty.NettyRemotingClient#createChannel
     */
    public Channel getOrCreateChannel(String endpoint) {
        Channel channel = getChannel(endpoint);
        if (channel != null) {
            return channel;
        }
        return createChannelWithLock(endpoint);
    }

    private Channel createChannelWithLock(String endpoint) {
        try {
            if (!this.lock.tryLock(3000, TimeUnit.MILLISECONDS)) {
                log.error("create channel {} wait timeout", endpoint);
                return null;
            }
            try {
                ChannelFuture channelFuture = this.channelFutureMap.get(endpoint);
                if (channelFuture == null) {
                    return createChannel(endpoint);
                }
                if (CoordinatorUtil.isActive(channelFuture)) {
                    return channelFuture.channel();
                }
                if (!channelFuture.isDone()) {
                    return awaitChannel(channelFuture);
                }
                this.channelFutureMap.remove(endpoint);
                return createChannel(endpoint);
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

    private Channel createChannel(String endpoint) {
        ChannelFuture channelFuture = this.channelFutureMap.computeIfAbsent(endpoint, k -> this.bootstrap.connect(CoordinatorUtil.toSocketAddress(k)));
        if (channelFuture != null) {
            return awaitChannel(channelFuture);
        }
        return null;
    }

    private Channel awaitChannel(ChannelFuture channelFuture) {
        if (channelFuture.awaitUninterruptibly(this.config.getConnectTimeoutMillis())) {
            if (CoordinatorUtil.isActive(channelFuture)) {
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
