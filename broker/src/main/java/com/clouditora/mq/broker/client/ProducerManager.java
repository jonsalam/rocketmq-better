package com.clouditora.mq.broker.client;

import com.clouditora.mq.network.util.CoordinatorUtil;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @link org.apache.rocketmq.broker.client.ProducerManager
 */
@Slf4j
public class ProducerManager {
    /**
     * @link org.apache.rocketmq.broker.client.ProducerManager#CHANNEL_EXPIRED_TIMEOUT
     */
    private static final long CHANNEL_EXPIRED_TIME = 120_000;
    /**
     * @link org.apache.rocketmq.broker.client.ProducerManager#groupChannelTable
     */
    private final ConcurrentMap<String, ClientChannel> producerMap = new ConcurrentHashMap<>();

    public void registerProducer(String group, ClientChannel clientChannel) {
        this.producerMap.put(group, clientChannel);
    }

    public void unregisterProducer(String group) {
        this.producerMap.remove(group);
    }

    /**
     * @link org.apache.rocketmq.broker.client.ProducerManager#scanNotActiveChannel
     */
    public void evictInactiveProducer() {
        Iterator<Map.Entry<String, ClientChannel>> it = this.producerMap.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, ClientChannel> next = it.next();
            ClientChannel info = next.getValue();
            if (System.currentTimeMillis() - info.getUpdateTime() > CHANNEL_EXPIRED_TIME) {
                Channel channel = info.getChannel();
                CoordinatorUtil.closeChannel(channel);
                it.remove();
                log.warn("producer {} expired", info.getClientId());
            }
        }
    }
}
