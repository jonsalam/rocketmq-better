package com.clouditora.mq.common.topic;

import com.alibaba.fastjson2.annotation.JSONField;
import lombok.Data;

/**
 * broker持久化topic用到的
 *
 * @link org.apache.rocketmq.common.TopicConfig
 */
@Data
public class TopicQueue {
    private static final String SEPARATOR = " ";

    @JSONField(name = "topicName")
    private String topic;

    @JSONField(name = "readQueueNums")
    private int readQueueNum = 16;

    @JSONField(name = "writeQueueNums")
    private int writeQueueNum = 16;

}
