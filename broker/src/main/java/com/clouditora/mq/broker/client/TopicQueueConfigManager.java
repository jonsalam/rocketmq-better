package com.clouditora.mq.broker.client;

import com.clouditora.mq.broker.BrokerConfig;
import com.clouditora.mq.broker.BrokerController;
import com.clouditora.mq.common.constant.RpcModel;
import com.clouditora.mq.common.constant.SystemTopic;
import com.clouditora.mq.common.service.AbstractFileService;
import com.clouditora.mq.common.topic.TopicQueueConfig;
import com.clouditora.mq.common.topic.TopicQueueConfigFile;
import com.clouditora.mq.common.util.JsonUtil;
import com.clouditora.mq.store.StoreConfig;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @link org.apache.rocketmq.broker.topic.TopicConfigManager
 */
@Slf4j
public class TopicQueueConfigManager extends AbstractFileService {
    private final BrokerController brokerController;
    @Getter
    private final ConcurrentMap<String, TopicQueueConfig> topicMap = new ConcurrentHashMap<>(1024);

    public TopicQueueConfigManager(BrokerConfig brokerConfig, StoreConfig storeConfig, BrokerController brokerController) {
        super("%s/config/topics.json".formatted(storeConfig.getRootPath()));
        this.brokerController = brokerController;

        {
            String topic = SystemTopic.SELF_TEST_TOPIC.getTopic();
            TopicQueueConfig topicQueueConfig = new TopicQueueConfig();
            topicQueueConfig.setTopic(topic);
            topicQueueConfig.setReadQueueNum(1);
            topicQueueConfig.setWriteQueueNum(1);
            this.topicMap.put(topic, topicQueueConfig);
        }
        {
            String topic = brokerConfig.getBrokerClusterName();
            SystemTopic.addSystemTopic(topic);
            TopicQueueConfig topicQueueConfig = new TopicQueueConfig();
            topicQueueConfig.setTopic(topic);
            topicQueueConfig.setReadQueueNum(16);
            topicQueueConfig.setWriteQueueNum(16);
            this.topicMap.put(topic, topicQueueConfig);
        }
        {
            String topic = brokerConfig.getBrokerName();
            SystemTopic.addSystemTopic(topic);
            TopicQueueConfig topicQueueConfig = new TopicQueueConfig();
            topicQueueConfig.setTopic(topic);
            topicQueueConfig.setReadQueueNum(1);
            topicQueueConfig.setWriteQueueNum(1);
            this.topicMap.put(topic, topicQueueConfig);
        }
    }

    @Override
    public String getServiceName() {
        return "TopicManager";
    }

    @Override
    protected void decode(String content) {
        TopicQueueConfigFile config = JsonUtil.toJsonObject(content, TopicQueueConfigFile.class);
        if (config != null) {
            this.topicMap.putAll(config.getConfigMap());
            print();
        }
    }

    private void print() {
        this.topicMap.forEach((topic, config) -> {
            log.info("load exist local topic {}={}", topic, config);
        });
    }

    @Override
    protected String encode() {
        TopicQueueConfigFile config = new TopicQueueConfigFile();
        config.setConfigMap(this.topicMap);
        return JsonUtil.toJsonStringPretty(config);
    }

    /**
     * @link org.apache.rocketmq.broker.topic.TopicConfigManager#createTopicInSendMessageBackMethod
     */
    public void registerTopic(String topic, int readQueueNum, int writeQueueNum) {
        this.topicMap.computeIfAbsent(topic, e -> {
            TopicQueueConfig topicQueueConfig = new TopicQueueConfig();
            topicQueueConfig.setTopic(topic);
            topicQueueConfig.setReadQueueNum(readQueueNum);
            topicQueueConfig.setWriteQueueNum(writeQueueNum);
            super.save();
            log.info("register topic {}: {}", topic, topicQueueConfig);
            brokerController.registerBroker(RpcModel.ONEWAY);
            return topicQueueConfig;
        });
    }

    public TopicQueueConfig get(String topic) {
        return this.topicMap.get(topic);
    }
}
