package com.clouditora.mq.client.consumer.offset;

import com.clouditora.mq.client.instance.ClientInstance;
import com.clouditora.mq.common.constant.GlobalConstant;
import com.clouditora.mq.common.topic.TopicQueue;
import lombok.extern.slf4j.Slf4j;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @link org.apache.rocketmq.client.consumer.store.RemoteBrokerOffsetStore
 */
@Slf4j
public class RemoteOffsetManager extends AbstractOffsetManager {
    private final ClientInstance clientInstance;

    public RemoteOffsetManager(String group, ClientInstance clientInstance) {
        super(group, null);
        this.clientInstance = clientInstance;
    }

    @Override
    public String getServiceName() {
        return RemoteOffsetManager.class.getSimpleName();
    }

    @Override
    protected void decode(String content) {

    }

    @Override
    protected String encode() {
        return null;
    }

    @Override
    public void update(TopicQueue queue, long offset) {

    }

    @Override
    public long get(TopicQueue queue) {
        long offset = super.get(queue);
        if (offset != -1) {
            return offset;
        }
        String endpoint = this.clientInstance.findEndpoint(queue.getBrokerName(), GlobalConstant.MASTER_ID, true);
        if (endpoint == null) {
            log.error("broker {} not available", queue.getBrokerName());
            return -1;
        }
        offset = this.clientInstance.getConsumerOffset();
        if (offset != -1) {
            super.update(queue, offset);
        }
        return offset;
    }

    @Override
    public void save() {
        Set<TopicQueue> set = new HashSet<>();
        for (Map.Entry<TopicQueue, AtomicLong> entry : super.offsetMap.entrySet()) {
            TopicQueue queue = entry.getKey();
            long offset = entry.getValue().get();
            log.info("save local consumer offset to broker: group={}, {}={}", super.group, queue, offset);
            String endpoint = this.clientInstance.findEndpoint(queue.getBrokerName(), GlobalConstant.MASTER_ID, true);
            if (endpoint == null) {
                log.error("broker {} not available", queue.getBrokerName());
                return;
            }
            this.clientInstance.uploadConsumerOffset();
            set.add(queue);
        }
        set.forEach(e -> this.offsetMap.remove(e));
    }
}
