package com.clouditora.mq.broker.client;

import com.clouditora.mq.common.topic.ProducerGroup;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @link org.apache.rocketmq.broker.client.ProducerManager
 */
@Slf4j
public class ProducerManager {
    /**
     * group:
     *
     * @link org.apache.rocketmq.broker.client.ProducerManager#groupChannelTable
     */
    private final ConcurrentMap<String, ClientChannelManager> channelMap = new ConcurrentHashMap<>();

    /**
     * @link org.apache.rocketmq.broker.client.ProducerManager#registerProducer
     */
    public void register(ClientChannel channel, Set<ProducerGroup> groups) {
        if (CollectionUtils.isEmpty(groups)) {
            return;
        }
        for (ProducerGroup group : groups) {
            ClientChannelManager manager = this.channelMap.computeIfAbsent(group.getGroup(), e -> {
                log.info("register producer group: {}", group.getGroup());
                return new ClientChannelManager(group.getGroup());
            });
            ClientChannel prevChannel = manager.add(channel.getChannel(), e -> {
                log.info("register producer channel: {}", channel);
                return channel;
            });
            prevChannel.setUpdateTime(channel.getUpdateTime());
        }
    }

    /**
     * WARNING: 只有主动注销了才移除group, channel断开和超时都不移除
     *
     * @link org.apache.rocketmq.broker.client.ProducerManager#unregisterProducer
     */
    public void unregister(ClientChannel channel, String group) {
        if (StringUtils.isBlank(group)) {
            return;
        }
        ClientChannelManager manager = this.channelMap.get(group);
        if (manager == null) {
            log.warn("unregister producer exception: {} not exists", group);
            return;
        }
        ClientChannel prev = manager.remove(channel.getChannel());
        if (prev != null) {
            log.info("unregister producer channel: {}", channel);
            if (manager.isEmpty()) {
                this.channelMap.remove(group);
                log.info("unregister producer group: {}", group);
            }
        }
    }

    /**
     * @link org.apache.rocketmq.broker.client.ProducerManager#doChannelCloseEvent
     */
    public void unregister(Channel channel) {
        for (Map.Entry<String, ClientChannelManager> entry : this.channelMap.entrySet()) {
            ClientChannelManager channelManager = entry.getValue();
            ClientChannel prev = channelManager.remove(channel);
            if (prev != null) {
                log.info("unregister producer channel: {}", channel);
            }
        }
    }

    /**
     * @link org.apache.rocketmq.broker.client.ProducerManager#scanNotActiveChannel
     */
    public void cleanExpiredClient() {
        this.channelMap.forEach((group, manager) -> manager.cleanExpiredChannel());
    }
}
