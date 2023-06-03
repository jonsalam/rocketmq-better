package com.clouditora.mq.client.consumer.pull;

import com.clouditora.mq.client.broker.BrokerQueueManager;
import com.clouditora.mq.client.consumer.Consumer;
import com.clouditora.mq.client.consumer.ConsumerConfig;
import com.clouditora.mq.client.consumer.consume.ConsumerQueue;
import com.clouditora.mq.client.consumer.offset.AbstractOffsetManager;
import com.clouditora.mq.client.instance.ClientInstance;
import com.clouditora.mq.common.constant.MessageModel;
import com.clouditora.mq.common.service.AbstractLaterService;
import com.clouditora.mq.common.topic.ConsumerSubscription;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @link org.apache.rocketmq.client.impl.consumer.PullMessageService
 */
@Slf4j
public class PullMessageService extends AbstractLaterService {
    /**
     * Flow control interval
     */
    private static final long PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL = 50;

    private final ConsumerConfig consumerConfig;
    private final ClientInstance clientInstance;
    private final AbstractOffsetManager offsetManager;
    private final BrokerQueueManager brokerQueueManager;
    private final LinkedBlockingQueue<PullMessageRequest> requestQueue = new LinkedBlockingQueue<>();

    public PullMessageService(ConsumerConfig consumerConfig, ClientInstance clientInstance, AbstractOffsetManager offsetManager, BrokerQueueManager brokerQueueManager) {
        this.consumerConfig = consumerConfig;
        this.clientInstance = clientInstance;
        this.offsetManager = offsetManager;
        this.brokerQueueManager = brokerQueueManager;
    }

    @Override
    public String getServiceName() {
        return PullMessageService.class.getSimpleName();
    }

    @Override
    protected void loop() throws Exception {
        PullMessageRequest request = this.requestQueue.take();
        executePullMessage(request);
    }

    public void pullMessage(PullMessageRequest request) {
        try {
            this.requestQueue.put(request);
        } catch (InterruptedException ignored) {
        }
    }

    public void pullMessageLater(PullMessageRequest request, long delay) {
        later(TimeUnit.MILLISECONDS, delay, () -> pullMessage(request));
    }

    /**
     * @link org.apache.rocketmq.client.impl.consumer.PullMessageService#pullMessage
     * @link org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl#pullMessage
     */
    private void executePullMessage(PullMessageRequest request) {
        Consumer consumer = this.clientInstance.selectConsumer(request.getGroup());
        if (consumer == null) {
            log.warn("no consumer for group: {}, request={}", request.getGroup(), request);
            return;
        }

        ConsumerQueue queue = request.getConsumerQueue();
        long messageCount = queue.getMessageCount();
        int macMessageCount = this.consumerConfig.getPullThresholdForQueue();
        if (messageCount > macMessageCount) {
            pullMessageLater(request, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL);
            log.warn("pull message later: local cached messages too much {}/{}, ", messageCount, macMessageCount);
            return;
        }
        long messageSize = queue.getMessageSize() / 1024 / 1024;
        int maxMessageSize = this.consumerConfig.getPullThresholdSizeForQueue();
        if (messageSize > maxMessageSize) {
            pullMessageLater(request, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL);
            log.warn("pull message later: local cached messages too big {}/{}MB", messageSize, maxMessageSize);
            return;
        }
        if (consumer.isOrderly()) {
            // 全局有序消息
            long span = queue.getMaxSpan();
            int maxSpan = this.consumerConfig.getConsumeConcurrentlyMaxSpan();
            if (span > maxSpan) {
                pullMessageLater(request, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL);
                log.warn("pull message later: local cached messages too span {}/{}", span, maxSpan);
                return;
            }
        } else {
            // 普通消息
            if (queue.isLocked()) {
                if (!request.isPreviouslyLocked()) {
                    long offset = -1L;
                    try {
                        offset = this.brokerQueueManager.getPullOffset(request.getTopicQueue());
                    } catch (Exception e) {
                        pullMessageLater(request, this.consumerConfig.getPullTimeDelayMillsWhenException());
                        log.warn("pull message later: failed get pull offset");
                        return;
                    }
                    log.info("");
                    if (request.getNextOffset() > offset) {
                        log.warn("");
                    }
                    request.setPreviouslyLocked(true);
                    request.setNextOffset(offset);
                }
            } else {
                pullMessageLater(request, this.consumerConfig.getPullTimeDelayMillsWhenException());
                log.warn("pull message later: not lock queue in broker");
                return;
            }
        }
        ConsumerSubscription subscription = consumer.getSubscription(request.getTopicQueue().getTopic());
        if (subscription == null) {
            pullMessageLater(request, this.consumerConfig.getPullTimeDelayMillsWhenException());
            log.warn("pull message later: consume subscription is null");
            return;
        }

        long offset = 0L;
        if (consumer.getMessageModel() == MessageModel.CLUSTERING) {
            offset = this.offsetManager.get(request.getTopicQueue(), ReadOffsetType.READ_FROM_MEMORY);
        }
        try {
            this.clientInstance.pullMessage(request, subscription, offset, this.consumerConfig.getPullBatchSize());
        } catch (Exception e) {
            pullMessageLater(request, this.consumerConfig.getPullTimeDelayMillsWhenException());
            log.error("pull message later: pull exception", e);
        }
    }
}
