package com.clouditora.mq.broker.client;

import com.clouditora.mq.broker.BrokerConfig;
import com.clouditora.mq.broker.MessageStoreConfig;
import com.clouditora.mq.common.constant.PermitBit;
import com.clouditora.mq.common.constant.SystemTopic;
import com.clouditora.mq.common.service.AbstractPersistentService;
import com.clouditora.mq.common.topic.TopicConfig;
import com.clouditora.mq.common.util.JsonUtil;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Slf4j
public class TopicManager extends AbstractPersistentService {
    @Getter
    private final ConcurrentMap<String, TopicConfig> topicMap = new ConcurrentHashMap<>(1024);

    public TopicManager(BrokerConfig brokerConfig) {
        super(brokerConfig.getMessageStoreConfig().getStorePathRootDir());

        {
            String topic = SystemTopic.SELF_TEST_TOPIC.getTopic();
            TopicConfig topicConfig = new TopicConfig(topic);
            topicConfig.setReadQueueNums(1);
            topicConfig.setWriteQueueNums(1);
            topicConfig.setPerm(PermitBit.RW);
            this.topicMap.put(topic, topicConfig);
        }
        {
            String topic = brokerConfig.getBrokerClusterName();
            SystemTopic.addSystemTopic(topic);
            TopicConfig topicConfig = new TopicConfig(topic);
            topicConfig.setReadQueueNums(16);
            topicConfig.setWriteQueueNums(16);
            topicConfig.setPerm(PermitBit.SYSTEM);
            this.topicMap.put(topic, topicConfig);
        }
        {
            String topic = brokerConfig.getBrokerName();
            SystemTopic.addSystemTopic(topic);
            TopicConfig topicConfig = new TopicConfig(topic);
            topicConfig.setReadQueueNums(1);
            topicConfig.setWriteQueueNums(1);
            this.topicMap.put(topic, topicConfig);
        }
    }

    @Override
    public String getServiceName() {
        return "TopicManager";
    }

    @Override
    protected void load(String content) {
        TopicConfigFile config = JsonUtil.parse(content, TopicConfigFile.class);
        if (config != null) {
            this.topicMap.putAll(config.getTopicConfigTable());
            print();
        }
    }

    private void print() {
        this.topicMap.forEach((topic, config) -> {
            log.info("load exist local topic {}={}", topic, config);
        });
    }

    @Override
    protected String persist() {
        TopicConfigFile config = new TopicConfigFile();
        config.setTopicConfigTable(this.topicMap);
        return JsonUtil.toJson(config);
    }
}
