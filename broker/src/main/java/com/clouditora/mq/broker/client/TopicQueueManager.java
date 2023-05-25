package com.clouditora.mq.broker.client;

import com.clouditora.mq.broker.BrokerConfig;
import com.clouditora.mq.broker.BrokerController;
import com.clouditora.mq.common.constant.RpcModel;
import com.clouditora.mq.common.constant.SystemTopic;
import com.clouditora.mq.common.service.AbstractPersistentService;
import com.clouditora.mq.common.topic.TopicQueue;
import com.clouditora.mq.common.topic.TopicQueueWrapper;
import com.clouditora.mq.common.util.JsonUtil;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @link org.apache.rocketmq.broker.topic.TopicConfigManager
 */
@Slf4j
public class TopicQueueManager extends AbstractPersistentService {
    private final BrokerController brokerController;
    @Getter
    private final ConcurrentMap<String, TopicQueue> topicMap = new ConcurrentHashMap<>(1024);

    public TopicQueueManager(BrokerConfig brokerConfig, BrokerController brokerController) {
        super("%s/config/topics.json".formatted(brokerConfig.getMessageStoreConfig().getRootPath()));
        this.brokerController = brokerController;

        {
            String topic = SystemTopic.SELF_TEST_TOPIC.getTopic();
            TopicQueue topicQueue = new TopicQueue();
            topicQueue.setTopic(topic);
            topicQueue.setReadQueueNum(1);
            topicQueue.setWriteQueueNum(1);
            this.topicMap.put(topic, topicQueue);
        }
        {
            String topic = brokerConfig.getBrokerClusterName();
            SystemTopic.addSystemTopic(topic);
            TopicQueue topicQueue = new TopicQueue();
            topicQueue.setTopic(topic);
            topicQueue.setReadQueueNum(16);
            topicQueue.setWriteQueueNum(16);
            this.topicMap.put(topic, topicQueue);
        }
        {
            String topic = brokerConfig.getBrokerName();
            SystemTopic.addSystemTopic(topic);
            TopicQueue topicQueue = new TopicQueue();
            topicQueue.setTopic(topic);
            topicQueue.setReadQueueNum(1);
            topicQueue.setWriteQueueNum(1);
            this.topicMap.put(topic, topicQueue);
        }
    }

    @Override
    public String getServiceName() {
        return "TopicManager";
    }

    @Override
    protected void load(String content) {
        TopicQueueWrapper config = JsonUtil.toJsonObject(content, TopicQueueWrapper.class);
        if (config != null) {
            this.topicMap.putAll(config.getTopicMap());
            print();
        }
    }

    private void print() {
        this.topicMap.forEach((topic, config) -> {
            log.info("load exist local topic {}={}", topic, config);
        });
    }

    @Override
    protected String unload() {
        TopicQueueWrapper config = new TopicQueueWrapper();
        config.setTopicMap(this.topicMap);
        return JsonUtil.toJsonStringPretty(config);
    }

    /**
     * @link org.apache.rocketmq.broker.topic.TopicConfigManager#createTopicInSendMessageBackMethod
     */
    public void registerTopic(String topic, int readQueueNum, int writeQueueNum) {
        this.topicMap.computeIfAbsent(topic, e -> {
            TopicQueue topicQueue = new TopicQueue();
            topicQueue.setTopic(topic);
            topicQueue.setReadQueueNum(readQueueNum);
            topicQueue.setWriteQueueNum(writeQueueNum);
            super.persist();
            log.info("register topic {}: {}", topic, topicQueue);
            brokerController.registerBroker(RpcModel.ONEWAY);
            return topicQueue;
        });
    }
}
