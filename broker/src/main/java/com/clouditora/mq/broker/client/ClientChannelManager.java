package com.clouditora.mq.broker.client;

import com.clouditora.mq.network.util.NetworkUtil;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

@Slf4j
public class ClientChannelManager {
    private final String group;
    /**
     * @link org.apache.rocketmq.broker.client.ConsumerManager#CHANNEL_EXPIRED_TIMEOUT
     */
    private static final long CHANNEL_EXPIRED_TIME = 120_000;

    private final ConcurrentMap<Channel, ClientChannel> channelMap = new ConcurrentHashMap<>();

    public ClientChannelManager(String group) {
        this.group = group;
    }

    public ClientChannel add(Channel channel, Function<Channel, ClientChannel> mapping) {
        return channelMap.computeIfAbsent(channel, mapping);
    }

    public ClientChannel remove(Channel channel) {
        return this.channelMap.remove(channel);
    }

    public boolean isEmpty() {
        return this.channelMap.isEmpty();
    }

    public Collection<ClientChannel> channels(){
        return this.channelMap.values();
    }

    /**
     * @link org.apache.rocketmq.broker.client.ConsumerManager#scanNotActiveChannel
     * @link org.apache.rocketmq.broker.client.ConsumerManager#scanNotActiveChannel
     */
    public void cleanExpiredChannel() {
        Iterator<Map.Entry<Channel, ClientChannel>> iterator = this.channelMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Channel, ClientChannel> next = iterator.next();
            ClientChannel channel = next.getValue();

            if (System.currentTimeMillis() - channel.getUpdateTime() > CHANNEL_EXPIRED_TIME) {
                NetworkUtil.closeChannel(channel.getChannel());
                iterator.remove();
                log.warn("client channel expired: group={}, channel={}", this.group, channel);
            }
        }
    }
}
