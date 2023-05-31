package com.clouditora.mq.client.consumer.offset;

import com.alibaba.fastjson2.annotation.JSONField;
import com.clouditora.mq.common.message.MessageQueue;
import lombok.Data;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @link org.apache.rocketmq.client.consumer.store.OffsetSerializeWrapper
 */
@Data
public class LocalOffsetFile {
    @JSONField(name = "offsetTable")
    private ConcurrentMap<MessageQueue, AtomicLong> offsetMap = new ConcurrentHashMap<>();
}
