package com.clouditora.mq.client.consumer.lb;

import com.clouditora.mq.client.consumer.offset.AbstractOffsetManager;
import com.clouditora.mq.common.constant.ConsumePositionStrategy;
import com.clouditora.mq.common.constant.GlobalConstant;
import com.clouditora.mq.common.exception.ClientException;
import com.clouditora.mq.common.message.MessageQueue;
import lombok.extern.slf4j.Slf4j;

/**
 * @link org.apache.rocketmq.client.impl.consumer.RebalancePushImpl
 */
@Slf4j
public class LoadBalance {

    /**
     * @link org.apache.rocketmq.client.impl.consumer.RebalancePushImpl#computePullFromWhereWithException
     */
    public long computePullFromWhereWithException(MessageQueue messageQueue) throws ClientException {
        long result = -1;
        ConsumePositionStrategy consumeFromWhere = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().getConsumeFromWhere();
        AbstractOffsetManager offsetStore = this.defaultMQPushConsumerImpl.getOffsetStore();
        switch (consumeFromWhere) {
            case FROM_LAST_OFFSET -> {
                // 如果broker上没有当前group的消费进度, 从maxOffset(最新的消息)处消费, 否则接着上次的消费进度
                long lastOffset = offsetStore.get(messageQueue, ReadOffsetType.READ_FROM_STORE);
                if (lastOffset >= 0) {
                    result = lastOffset;
                } else if (lastOffset == -1) {
                    // 如果是重试队列，就从0开始消费
                    if (messageQueue.getTopic().startsWith(GlobalConstant.SystemGroup.RETRY_GROUP_TOPIC_PREFIX)) {
                        result = 0L;
                    } else {
                        try {
                            // 从maxOffset(最新的消息)处消费
                            result = this.mQClientFactory.getMQAdminImpl().maxOffset(messageQueue);
                        } catch (ClientException e) {
                            log.warn("Compute consume offset from last offset exception, mq={}, exception={}", messageQueue, e);
                            throw e;
                        }
                    }
                } else {
                    result = -1;
                }
            }
            case FROM_FIRST_OFFSET -> {
                // 如果broker上没有当前group的消费进度, 从0(第一条消息)处消费, 否则接着上次的消费进度
                long lastOffset = offsetStore.get(messageQueue, ReadOffsetType.READ_FROM_STORE);
                if (lastOffset >= 0) {
                    result = lastOffset;
                } else if (lastOffset == -1) {
                    result = 0L;
                } else {
                    result = -1;
                }
            }
            case FROM_TIMESTAMP -> {
                long lastOffset = offsetStore.get(messageQueue, ReadOffsetType.READ_FROM_STORE);
                if (lastOffset >= 0) {
                    result = lastOffset;
                } else if (lastOffset == -1) {
                    if (messageQueue.getTopic().startsWith(GlobalConstant.SystemGroup.RETRY_GROUP_TOPIC_PREFIX)) {
                        try {
                            result = this.mQClientFactory.getMQAdminImpl().maxOffset(messageQueue);
                        } catch (ClientException e) {
                            log.warn("Compute consume offset from last offset exception, mq={}, exception={}", messageQueue, e);
                            throw e;
                        }
                    } else {
                        try {
                            long timestamp = UtilAll.parseDate(this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer().getConsumeTimestamp(), UtilAll.YYYYMMDDHHMMSS).getTime();
                            result = this.mQClientFactory.getMQAdminImpl().searchOffset(messageQueue, timestamp);
                        } catch (ClientException e) {
                            log.warn("Compute consume offset from last offset exception, mq={}, exception={}", messageQueue, e);
                            throw e;
                        }
                    }
                } else {
                    result = -1;
                }
            }
            default -> {
            }
        }
        return result;
    }
}
