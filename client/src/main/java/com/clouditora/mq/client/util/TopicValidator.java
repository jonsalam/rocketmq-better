package com.clouditora.mq.client.util;

import com.clouditora.mq.client.exception.MqClientException;
import com.clouditora.mq.common.constant.SystemTopic;
import org.apache.commons.lang3.StringUtils;

/**
 * @link org.apache.rocketmq.client.Validators
 */
public class TopicValidator {
    public static final int CHARACTER_MAX_LENGTH = 255;
    public static final int TOPIC_MAX_LENGTH = 127;

    /**
     * @link org.apache.rocketmq.client.Validators#checkTopic
     * @link org.apache.rocketmq.client.Validators#isSystemTopic
     */
    public static void checkTopic(String topic) throws MqClientException {
        if (StringUtils.isBlank(topic)) {
            throw new MqClientException("topic is blank");
        }
        if (topic.length() > TOPIC_MAX_LENGTH) {
            throw new MqClientException(String.format("topic [%s] over length: %d/%d", topic, topic.length(), TOPIC_MAX_LENGTH));
        }
        if (!SystemTopic.validCharacters(topic)) {
            throw new MqClientException(String.format("topic [%s] contains illegal characters, only: ^[%%|a-zA-Z0-9_-]+$", topic));
        }
        if (SystemTopic.isSystemTopic(topic)) {
            throw new MqClientException(String.format("topic [%s] conflicts with system topic.", topic));
        }
    }
}
