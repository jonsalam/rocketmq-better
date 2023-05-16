package com.clouditora.mq.common.topic;

import com.alibaba.fastjson2.annotation.JSONField;
import lombok.Data;

/**
 * @link org.apache.rocketmq.common.protocol.heartbeat.ProducerData
 */
@Data
public class ProducerGroup {
    @JSONField(name = "groupName")
    private String group;
}
