package com.clouditora.mq.broker.client;

import com.clouditora.mq.network.util.CoordinatorUtil;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @link org.apache.rocketmq.broker.client.ConsumerManager
 */
@Slf4j
public class ConsumerManager {
    /**
     * @link org.apache.rocketmq.broker.client.ConsumerManager#CHANNEL_EXPIRED_TIMEOUT
     */
    private static final long CHANNEL_EXPIRED_TIME = 120_000;
    /**
     * @link org.apache.rocketmq.broker.client.ConsumerManager#consumerTable
     */
    private final ConcurrentMap<String, ClientChannel> consumerMap = new ConcurrentHashMap<>();

    public void registerConsumer(String group, ClientChannel clientChannel) {
        this.consumerMap.put(group, clientChannel);
    }

    public void unregisterConsumer(String group) {
        this.consumerMap.remove(group);
    }

    /**
     * @link org.apache.rocketmq.broker.client.ProducerManager#scanNotActiveChannel
     */
    public void evictInactiveConsumer() {
        Iterator<Map.Entry<String, ClientChannel>> it = this.consumerMap.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, ClientChannel> next = it.next();
            ClientChannel info = next.getValue();
            if (System.currentTimeMillis() - info.getUpdateTime() > CHANNEL_EXPIRED_TIME) {
                Channel channel = info.getChannel();
                CoordinatorUtil.closeChannel(channel);
                it.remove();
                log.warn("consumer {} expired", info.getClientId());
            }
        }
    }
}
