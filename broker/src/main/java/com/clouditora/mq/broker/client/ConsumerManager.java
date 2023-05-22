package com.clouditora.mq.broker.client;

import com.clouditora.mq.common.constant.GlobalConstant;
import com.clouditora.mq.common.topic.ConsumerSubscriptions;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @link org.apache.rocketmq.broker.client.ConsumerManager
 */
@Slf4j
public class ConsumerManager {
    /**
     * @link org.apache.rocketmq.broker.topic.TopicConfigManager
     */
    private final TopicQueueManager topicQueueManager;
    /**
     * group:
     *
     * @link org.apache.rocketmq.broker.client.ConsumerGroupInfo#channelInfoTable
     */
    private final ConcurrentMap<String, ClientChannelManager> channelMap = new ConcurrentHashMap<>();
    /**
     * group:
     *
     * @link org.apache.rocketmq.broker.client.ConsumerManager#consumerTable
     */
    private final ConcurrentMap<String, ConsumerSubscribeManager> subscriptionMap = new ConcurrentHashMap<>();

    public ConsumerManager(TopicQueueManager topicQueueManager) {
        this.topicQueueManager = topicQueueManager;
    }

    /**
     * @link org.apache.rocketmq.broker.client.ConsumerManager#registerConsumer
     */
    public void register(ClientChannel channel, Set<ConsumerSubscriptions> gourps) {
        if (CollectionUtils.isEmpty(gourps)) {
            return;
        }
        for (ConsumerSubscriptions sub : gourps) {
            String group = sub.getGroup();
            // 注册重试topic
            {
                String retryTopic = GlobalConstant.SystemGroup.wrapRetry(group);
                topicQueueManager.registerTopic(retryTopic, 1, 1);
            }
            // 更新订阅
            {
                ConsumerSubscribeManager manager = this.subscriptionMap.computeIfAbsent(group, e -> {
                    log.info("register consumer group: {}", sub);
                    return new ConsumerSubscribeManager(group, sub.getMessageModel(), sub.getPositionStrategy());
                });
                manager.update(sub);
            }
            // 更新channel
            {
                ClientChannelManager manager = this.channelMap.computeIfAbsent(group, e -> {
                    log.info("register consumer group: {}", group);
                    return new ClientChannelManager(group);
                });
                ClientChannel prevChannel = manager.add(channel.getChannel(), e -> {
                    log.info("register consumer channel: {}", channel);
                    return channel;
                });
                prevChannel.setUpdateTime(channel.getUpdateTime());
                if (!prevChannel.getClientId().equals(channel.getClientId())) {
                    log.error("[BUG] consumer channel exist in broker, but clientId not equal. GROUP={}, OLD={}, NEW={} ", sub, prevChannel, channel);
                }
            }
        }
    }

    /**
     * WARNING: 只有主动注销了才移除group, channel断开和超时都不移除
     * WARNING: 注销的时候并没有移除订阅
     *
     * @link org.apache.rocketmq.broker.client.ConsumerManager#registerConsumer
     */
    public void unregister(ClientChannel channel, String group) {
        if (StringUtils.isBlank(group)) {
            return;
        }
        ClientChannelManager manager = this.channelMap.get(group);
        if (manager == null) {
            log.warn("unregister consumer exception: {} not exists", group);
            return;
        }
        ClientChannel prev = manager.remove(channel.getChannel());
        if (prev != null) {
            log.info("unregister consumer channel: {}", channel);
            if (manager.isEmpty()) {
                this.channelMap.remove(group);
                log.info("unregister consumer group: {}", group);
            }
        }
    }

    /**
     * @link org.apache.rocketmq.broker.client.ConsumerManager#doChannelCloseEvent
     */
    public void unregister(Channel channel) {
        for (Map.Entry<String, ClientChannelManager> entry : this.channelMap.entrySet()) {
            ClientChannelManager channelManager = entry.getValue();
            ClientChannel prev = channelManager.remove(channel);
            if (prev != null) {
                log.info("unregister consumer channel: {}", channel);
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
