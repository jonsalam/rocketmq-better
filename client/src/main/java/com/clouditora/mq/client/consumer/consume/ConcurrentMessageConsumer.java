package com.clouditora.mq.client.consumer.consume;

import com.clouditora.mq.client.consumer.ConsumerConfig;
import com.clouditora.mq.client.consumer.listener.ConcurrentMessageListener;
import com.clouditora.mq.common.message.MessageEntity;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 * @link org.apache.rocketmq.client.impl.consumer.ConsumeMessageConcurrentlyService
 */
@Slf4j
public class ConcurrentMessageConsumer extends AbstractMessageConsumer {
    private final ConcurrentMessageListener messageListener;

    public ConcurrentMessageConsumer(ConsumerConfig config, ConcurrentMessageListener messageListener) {
        super(config);
        this.messageListener = messageListener;
    }

    @Override
    public String getServiceName() {
        return "ConcurrentMessageConsumer";
    }

    @Override
    public void startup() {
        scheduled(TimeUnit.MINUTES, super.config.getConsumeTimeout(), super.config.getConsumeTimeout(), this::cleanExpiredMessages);
        super.startup();
    }

    /**
     * @link org.apache.rocketmq.client.impl.consumer.ConsumeMessageConcurrentlyService#cleanExpireMsg
     * @link org.apache.rocketmq.client.impl.consumer.ProcessQueue#cleanExpiredMsg
     */
    private void cleanExpiredMessages() {
        if (pushConsumer.getDefaultMQPushConsumerImpl().isConsumeOrderly()) {
            return;
        }

        int loop = msgTreeMap.size() < 16 ? msgTreeMap.size() : 16;
        for (int i = 0; i < loop; i++) {
            MessageExt msg = null;
            try {
                this.treeMapLock.readLock().lockInterruptibly();
                try {
                    if (!msgTreeMap.isEmpty()) {
                        String consumeStartTimeStamp = MessageAccessor.getConsumeStartTimeStamp(msgTreeMap.firstEntry().getValue());
                        if (StringUtils.isNotEmpty(consumeStartTimeStamp) && System.currentTimeMillis() - Long.parseLong(consumeStartTimeStamp) > pushConsumer.getConsumeTimeout() * 60 * 1000) {
                            msg = msgTreeMap.firstEntry().getValue();
                        } else {
                            break;
                        }
                    } else {
                        break;
                    }
                } finally {
                    this.treeMapLock.readLock().unlock();
                }
            } catch (InterruptedException e) {
                log.error("getExpiredMsg exception", e);
            }

            try {

                pushConsumer.sendMessageBack(msg, 3);
                log.info("send expire msg back. topic={}, msgId={}, storeHost={}, queueId={}, queueOffset={}", msg.getTopic(), msg.getMsgId(), msg.getStoreHost(), msg.getQueueId(), msg.getQueueOffset());
                try {
                    this.treeMapLock.writeLock().lockInterruptibly();
                    try {
                        if (!msgTreeMap.isEmpty() && msg.getQueueOffset() == msgTreeMap.firstKey()) {
                            try {
                                removeMessage(Collections.singletonList(msg));
                            } catch (Exception e) {
                                log.error("send expired msg exception", e);
                            }
                        }
                    } finally {
                        this.treeMapLock.writeLock().unlock();
                    }
                } catch (InterruptedException e) {
                    log.error("getExpiredMsg exception", e);
                }
            } catch (Exception e) {
                log.error("send expired msg exception", e);
            }
        }
    }

    /**
     * @link org.apache.rocketmq.client.impl.consumer.ConsumeMessageConcurrentlyService#submitConsumeRequest
     */
    public void consume(MessageEntity message) {
        ConsumeResult result = new ConsumeResult();
        try {
            ConsumeStatus status = messageListener.consume(message);
            if (status == null) {
                result.set
            }
        } catch (Exception e) {

        }
    }

    /**
     * @link org.apache.rocketmq.client.impl.consumer.ConsumeMessageConcurrentlyService#submitConsumeRequestLater
     */
    public void consumeLater(MessageEntity message) {

    }
}
