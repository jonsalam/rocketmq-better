package com.clouditora.mq.network.client;

import com.clouditora.mq.common.service.AbstractScheduledService;
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
public class ClientNameServerManager extends AbstractScheduledService {
    protected final ClientChannelHolder channelHolder;
    /**
     * @link org.apache.rocketmq.remoting.netty.NettyRemotingClient#namesrvAddrList
     */
    protected final AtomicReference<List<String>> nameServerEndpoints = new AtomicReference<>();
    /**
     * @link org.apache.rocketmq.remoting.netty.NettyRemotingClient#namesrvIndex
     */
    protected final AtomicInteger nameServerEndpointIndex = new AtomicInteger(random());
    /**
     * @link org.apache.rocketmq.remoting.netty.NettyRemotingClient#namesrvAddrChoosed
     */
    protected final AtomicReference<String> currentNameServerEndpoint = new AtomicReference<>();
    /**
     * @link org.apache.rocketmq.remoting.netty.NettyRemotingClient#namesrvChannelLock
     */
    protected final Lock lock = new ReentrantLock();
    protected final Runnable scheduledRunnable;

    public ClientNameServerManager(ClientChannelHolder channelHolder, Runnable scheduledRunnable) {
        this.channelHolder = channelHolder;
        this.scheduledRunnable = scheduledRunnable;
    }

    @Override
    public String getServiceName() {
        return "NameServerService";
    }

    /**
     * Allowing values are between 10, 000 and 60, 000 milliseconds.
     *
     * @link org.apache.rocketmq.common.BrokerConfig#registerNameServerPeriod
     */
    @Override
    public void startup() {
        register(TimeUnit.SECONDS, 10, 30, scheduledRunnable);
    }

    private static int random() {
        return Math.abs(new Random().nextInt() % 999) % 999;
    }

    public List<String> getNameServerEndpoints() {
        return Optional.ofNullable(this.nameServerEndpoints.get()).orElse(List.of());
    }

    public void updateNameServerEndpoints(List<String> list) {
        Collections.shuffle(list);
        this.nameServerEndpoints.set(list);
    }

    /**
     * @link org.apache.rocketmq.remoting.netty.NettyRemotingClient#getAndCreateNameserverChannel
     */
    public Channel getOrCreateChannel() {
        Channel channel = getCurrentChannel();
        if (channel != null) {
            return null;
        }
        try {
            if (!this.lock.tryLock(3000, TimeUnit.MILLISECONDS)) {
                log.error("[channel] create name server wait timeout");
                return null;
            }
            try {
                channel = getCurrentChannel();
                if (channel != null) {
                    return null;
                }
                List<String> list = nameServerEndpoints.get();
                for (String ignored : list) {
                    int index = randomIndex(list);
                    String endpoint = list.get(index);
                    channel = channelHolder.getOrCreateChannel(endpoint);
                    if (channel != null) {
                        this.currentNameServerEndpoint.set(endpoint);
                        return channel;
                    }
                }
            } catch (Exception e) {
                log.error("[channel] create name server exception", e);
            } finally {
                this.lock.unlock();
            }
        } catch (InterruptedException e) {
            log.error("[channel] create name server exception", e);
        }
        return null;
    }

    private int randomIndex(List<String> list) {
        int index = Math.abs(nameServerEndpointIndex.incrementAndGet());
        return index % list.size();
    }

    public Channel getOrCreateChannel(String endpoint) {
        return channelHolder.getOrCreateChannel(endpoint);
    }

    private Channel getCurrentChannel() {
        String endpoint = this.currentNameServerEndpoint.get();
        if (endpoint == null) {
            return null;
        }
        return channelHolder.getChannel(endpoint);
    }

    public void closeChannel(Channel channel) {
        this.channelHolder.closeChannel(channel);
    }
}
