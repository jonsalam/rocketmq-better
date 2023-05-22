package com.clouditora.mq.network.client;

import com.clouditora.mq.network.ClientNetworkConfig;
import com.clouditora.mq.network.exception.ConnectException;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
public class ClientChannelPool {
    protected final ClientChannelCache channelCache;
    /**
     * @link org.apache.rocketmq.remoting.netty.NettyRemotingClient#namesrvAddrList
     */
    protected final AtomicReference<List<String>> nameserverEndpoints = new AtomicReference<>();
    /**
     * @link org.apache.rocketmq.remoting.netty.NettyRemotingClient#namesrvIndex
     */
    protected final AtomicInteger nameserverEndpointIndex = new AtomicInteger(Math.abs(new Random().nextInt() % 999) % 999);
    /**
     * @link org.apache.rocketmq.remoting.netty.NettyRemotingClient#namesrvAddrChoosed
     */
    protected final AtomicReference<String> currentNameserverEndpoint = new AtomicReference<>();
    /**
     * @link org.apache.rocketmq.remoting.netty.NettyRemotingClient#namesrvChannelLock
     */
    protected final Lock lock = new ReentrantLock();

    public ClientChannelPool(ClientNetworkConfig config, Bootstrap bootstrap) {
        this.channelCache = new ClientChannelCache(config, bootstrap::connect);
    }

    public List<String> getNameserverEndpoints() {
        return Optional.ofNullable(this.nameserverEndpoints.get()).orElse(List.of());
    }

    /**
     * @link org.apache.rocketmq.remoting.netty.NettyRemotingClient#updateNameServerAddressList
     */
    public void updateNameserverEndpoints(List<String> list) {
        Collections.shuffle(list);
        this.nameserverEndpoints.set(list);
    }

    public void closeChannel(Channel channel) {
        this.channelCache.closeChannel(channel);
    }

    /**
     * @param endpoint null means get nameserver channel
     * @link org.apache.rocketmq.remoting.netty.NettyRemotingClient#getAndCreateChannel
     */
    public Channel createChannel(String endpoint) {
        if (endpoint == null) {
            return createNameserverChannel();
        }
        return this.channelCache.createChannel(endpoint);
    }

    /**
     * @link org.apache.rocketmq.remoting.netty.NettyRemotingClient#getAndCreateNameserverChannel
     */
    protected Channel createNameserverChannel() {
        Channel channel = getCurrentNameserverChannel();
        if (channel != null) {
            return channel;
        }
        try {
            if (!this.lock.tryLock(ClientChannelCache.LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                log.error("[channel] create nameserver channel wait timeout");
                return null;
            }
            try {
                // double check, try again
                channel = getCurrentNameserverChannel();
                if (channel != null) {
                    return channel;
                }

                List<String> list = this.nameserverEndpoints.get();
                for (String ignored : list) {
                    int index = nextIndex(list);
                    String endpoint = list.get(index);
                    channel = this.channelCache.createChannel(endpoint);
                    if (channel != null) {
                        this.currentNameserverEndpoint.set(endpoint);
                        log.info("[channel] nameserver {} change to {}", this.currentNameserverEndpoint.get(), endpoint);
                        return channel;
                    }
                }
                throw new ConnectException(list.toString());
            } catch (Exception e) {
                log.error("[channel] create nameserver channel exception", e);
            } finally {
                this.lock.unlock();
            }
        } catch (InterruptedException e) {
            log.error("[channel] create nameserver channel exception", e);
        }
        return null;
    }

    protected int nextIndex(List<String> list) {
        int index = Math.abs(this.nameserverEndpointIndex.incrementAndGet());
        return index % list.size();
    }

    protected Channel getCurrentNameserverChannel() {
        String endpoint = this.currentNameserverEndpoint.get();
        if (endpoint == null) {
            return null;
        }
        return this.channelCache.getChannel(endpoint);
    }
}
