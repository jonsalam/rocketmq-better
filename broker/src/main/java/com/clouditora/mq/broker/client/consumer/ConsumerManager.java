package com.clouditora.mq.broker.client.consumer;

import com.clouditora.mq.broker.client.ClientChannel;
import com.clouditora.mq.broker.client.ClientChannelManager;
import com.clouditora.mq.broker.client.TopicQueueConfigManager;
import com.clouditora.mq.common.constant.GlobalConstant;
import com.clouditora.mq.common.topic.GroupSubscription;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * @link org.apache.rocketmq.broker.client.ConsumerManager
 */
@Slf4j
public class ConsumerManager {
    /**
     * @link org.apache.rocketmq.broker.topic.TopicConfigManager
     */
    private final TopicQueueConfigManager topicQueueConfigManager;
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

    public ConsumerManager(TopicQueueConfigManager topicQueueConfigManager) {
        this.topicQueueConfigManager = topicQueueConfigManager;
    }

    /**
     * @link org.apache.rocketmq.broker.client.ConsumerManager#registerConsumer
     */
    public void register(ClientChannel channel, Set<GroupSubscription> groups) {
        if (CollectionUtils.isEmpty(groups)) {
            return;
        }
        for (GroupSubscription sub : groups) {
            String group = sub.getGroup();
            // 注册重试topic
            {
                String retryTopic = GlobalConstant.SystemGroup.wrapRetry(group);
                topicQueueConfigManager.registerTopic(retryTopic, 1, 1);
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

    public ConsumerSubscribeManager getConsumerSubscription(String group) {
        return this.subscriptionMap.get(group);
    }

    /**
     * @link org.apache.rocketmq.broker.client.ProducerManager#scanNotActiveChannel
     */
    public void cleanExpiredClient() {
        this.channelMap.forEach((group, manager) -> manager.cleanExpiredChannel());
    }

    /**
     * @link org.apache.rocketmq.broker.client.ConsumerManager#getConsumerGroupInfo
     * @link org.apache.rocketmq.broker.client.ConsumerGroupInfo#getAllClientId
     */
    public List<String> findConsumerIdsByGroup(String group) {
        return this.channelMap.get(group)
                .channels()
                .stream()
                .map(ClientChannel::getClientId)
                .collect(Collectors.toList());
    }
}
