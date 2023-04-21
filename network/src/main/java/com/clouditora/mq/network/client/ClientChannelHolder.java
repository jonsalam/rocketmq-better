package com.clouditora.mq.network.client;

import com.clouditora.mq.network.ClientNetworkConfig;
import com.clouditora.mq.network.util.CoordinatorUtil;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

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

    public ClientChannelHolder(ClientNetworkConfig config, Bootstrap bootstrap, ConcurrentMap<String, ChannelFuture> channelFutureMap) {
        this.config = config;
        this.bootstrap = bootstrap;
        this.channelFutureMap = channelFutureMap;
    }

    /**
     * @link org.apache.rocketmq.remoting.netty.NettyRemotingClient#closeChannel(io.netty.channel.Channel)
     */
    public void closeChannel(Channel channel) {
        if (channel == null) {
            return;
        }
        try {
            String address = CoordinatorUtil.toAddress(channel);
            if (!this.lock.tryLock(3000, TimeUnit.MILLISECONDS)) {
                log.error("[channel] close {} wait timeout", address);
                return;
            }
            try {
                ChannelFuture channelFuture = this.channelFutureMap.get(address);
                if (channelFuture != null && channelFuture.channel() == channel) {
                    this.channelFutureMap.remove(address);
                    CoordinatorUtil.closeChannel(channel);
                }
            } catch (Exception e) {
                log.error("[channel] close {} exception", CoordinatorUtil.toAddress(channel), e);
            } finally {
                this.lock.unlock();
            }
        } catch (InterruptedException e) {
            log.error("[channel] close {} exception", CoordinatorUtil.toAddress(channel), e);
        }
    }

    public Channel getChannel(String address) {
        ChannelFuture channelFuture = this.channelFutureMap.get(address);
        if (CoordinatorUtil.isActive(channelFuture)) {
            return channelFuture.channel();
        }
        return null;
    }

    /**
     * @link org.apache.rocketmq.remoting.netty.NettyRemotingClient#createChannel
     */
    public Channel getOrCreateChannel(String address) {
        Channel channel = getChannel(address);
        if (channel != null) {
            return channel;
        }
        ChannelFuture channelFuture = createChannelFutureWithLock(address);
        if (channelFuture != null) {
            if (channelFuture.awaitUninterruptibly(this.config.getConnectTimeoutMillis())) {
                if (CoordinatorUtil.isActive(channelFuture)) {
                    log.info("[channel] {} create success", address);
                    return channelFuture.channel();
                } else {
                    log.error("[channel] {} create failed", address);
                }
            } else {
                log.error("[channel] {} create timeout", address);
            }
        }
        return null;
    }

    private ChannelFuture createChannelFutureWithLock(String address) {
        try {
            if (!this.lock.tryLock(3000, TimeUnit.MILLISECONDS)) {
                log.error("[channel] create {} wait timeout", address);
                return null;
            }
            try {
                ChannelFuture channelFuture = this.channelFutureMap.get(address);
                if (channelFuture == null) {
                    return createChannelFuture(address);
                }
                if (CoordinatorUtil.isActive(channelFuture)) {
                    return channelFuture;
                }
                if (channelFuture.isDone()) {
                    this.channelFutureMap.remove(address);
                    return createChannelFuture(address);
                }
            } catch (Exception e) {
                log.error("[channel] create {} exception", address, e);
            } finally {
                this.lock.unlock();
            }
        } catch (InterruptedException e) {
            log.error("[channel] create {} exception", address, e);
        }
        return null;
    }

    private ChannelFuture createChannelFuture(String address) {
        return this.channelFutureMap.computeIfAbsent(address, k -> this.bootstrap.connect(CoordinatorUtil.toSocketAddress(k)));
    }
}
