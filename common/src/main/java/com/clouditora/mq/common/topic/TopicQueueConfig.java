package com.clouditora.mq.common.topic;

import com.alibaba.fastjson2.annotation.JSONField;
import lombok.Data;

/**
 * broker持久化topic用到的
 * 不同于 @link com.clouditora.mq.common.broker.BrokerQueue
 *
 * @link org.apache.rocketmq.common.TopicConfig
 */
@Data
public class TopicQueueConfig {
    private static final String SEPARATOR = " ";

    @JSONField(name = "topicName")
    private String topic;
    @JSONField(name = "readQueueNums")
    private int readQueueNum = 16;
    @JSONField(name = "writeQueueNums")
    private int writeQueueNum = 16;

}
