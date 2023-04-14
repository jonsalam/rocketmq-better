package com.clouditora.mq.client.consumer;

import com.clouditora.mq.client.MqConsumer;
import com.clouditora.mq.common.constant.MessageModel;
import com.clouditora.mq.common.service.AbstractLaterService;
import com.clouditora.mq.common.service.PauseService;
import lombok.extern.slf4j.Slf4j;

/**
 * @link org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl
 */
@Slf4j
public class DefaultMqConsumer extends AbstractLaterService implements PauseService, MqConsumer {
    private final ConsumerConfig consumerConfig;

    protected final PullMessageService pullMessageService;

    private volatile boolean pause = false;
    private boolean consumeOrderly = false;
    /**
     * Consumers of the same role is required to have exactly same subscriptions and consumerGroup to correctly achieve
     * load balance. It's required and needs to be globally unique.
     * See <a href="http://rocketmq.apache.org/docs/core-concept/">here</a> for further discussion.
     *
     * @link org.apache.rocketmq.client.consumer.DefaultMQPushConsumer#consumerGroup
     */
    private String consumerGroup;

    public DefaultMqConsumer(ConsumerConfig consumerConfig, PullMessageService pullMessageService) {
        this.consumerConfig = consumerConfig;
        this.pullMessageService = pullMessageService;
    }

    @Override
    public String getServiceName() {
        return "PushConsumer";
    }

    @Override
    public boolean isPause() {
        return pause;
    }

    @Override
    public void pause() {
        this.pause = true;
    }

    @Override
    public void recover() {
        this.pause = false;
    }

    private String getInstanceName() {
        return null;
    }

    /**
     * @link org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl#pullMessage
     */
    public void pullMessage(PullRequest pullRequest) {
        ProcessQueue processQueue = pullRequest.getProcessQueue();
        if (processQueue.isDropped()) {
            log.info("the pull request[{}] is dropped.", pullRequest);
            return;
        }

        // MARK 为什么先设置时间
        pullRequest.getProcessQueue().setLastPullTimestamp(System.currentTimeMillis());

        boolean state = checkServiceState(pullRequest);
        if (!state) {
            return;
        }
        boolean limit = checkLimit(pullRequest, processQueue);
        if (!limit) {
            return;
        }

        if (this.consumeOrderly) {
            if (!processQueue.isLocked()) {
                pullMessageLater(pullRequest, this.consumerConfig.getPullTimeDelayMillsWhenException());
                log.info("pull request later: orderly message but queue not locked");
                return;
            } else {
                if (!pullRequest.isPreviouslyLocked()) {
                    long offset = -1L;
                    try {
                        offset = this.rebalanceImpl.computePullFromWhereWithException(pullRequest.getMessageQueue());
                    } catch (Exception e) {
                        pullMessageLater(pullRequest, this.consumerConfig.getPullTimeDelayMillsWhenException());
                        log.error("Failed to compute pull offset, pullResult: {}", pullRequest, e);
                        return;
                    }
                    boolean brokerBusy = offset < pullRequest.getNextOffset();
                    log.info("the first time to pull message, so fix offset from broker. pullRequest: {} NewOffset: {} brokerBusy: {}", pullRequest, offset, brokerBusy);
                    if (brokerBusy) {
                        log.info("[NOTIFYME]the first time to pull message, but pull request offset larger than broker consume offset. pullRequest: {} NewOffset: {}", pullRequest, offset);
                    }

                    pullRequest.setPreviouslyLocked(true);
                    pullRequest.setNextOffset(offset);
                }
            }
        } else {
            if (processQueue.getMaxSpan() > this.defaultMQPushConsumer.getConsumeConcurrentlyMaxSpan()) {
                pullMessageLater(pullRequest, ConsumerConfig.PULL_TIME_DELAY_MILLS_WHEN_CACHE_FLOW_CONTROL);
                log.warn(
                        "the queue's messages, span too long, so do flow control, minOffset={}, maxOffset={}, maxSpan={}, pullRequest={}, flowControlTimes={}",
                        processQueue.getMsgTreeMap().firstKey(), processQueue.getMsgTreeMap().lastKey(), processQueue.getMaxSpan(),
                        pullRequest, queueMaxSpanFlowControlTimes);
                return;
            }
        }

        SubscriptionData subscriptionData = this.rebalanceImpl.getSubscriptionInner().get(pullRequest.getMessageQueue().getTopic());
        if (subscriptionData == null) {
            pullMessageLater(pullRequest, this.consumerConfig.getPullTimeDelayMillsWhenException());
            log.warn("find the consumer's subscription failed, {}", pullRequest);
            return;
        }
        long beginTimestamp = System.currentTimeMillis();
        PullCallback pullCallback = new PullCallback() {
            @Override
            public void onSuccess(PullResult pullResult) {
                if (pullResult == null) {
                    return;
                }
                pullResult = DefaultMQPushConsumerImpl.this.pullAPIWrapper.processPullResult(pullRequest.getMessageQueue(), pullResult, subscriptionData);

                switch (pullResult.getPullStatus()) {
                    case FOUND:
                        long prevRequestOffset = pullRequest.getNextOffset();
                        pullRequest.setNextOffset(pullResult.getNextBeginOffset());
                        long pullRT = System.currentTimeMillis() - beginTimestamp;
                        DefaultMQPushConsumerImpl.this.getConsumerStatsManager().incPullRT(pullRequest.getConsumerGroup(), pullRequest.getMessageQueue().getTopic(), pullRT);
                        long firstMsgOffset = Long.MAX_VALUE;
                        if (pullResult.getMsgFoundList() == null || pullResult.getMsgFoundList().isEmpty()) {
                            DefaultMQPushConsumerImpl.this.executePullRequestImmediately(pullRequest);
                        } else {
                            firstMsgOffset = pullResult.getMsgFoundList().get(0).getQueueOffset();
                            DefaultMQPushConsumerImpl.this.getConsumerStatsManager().incPullTPS(pullRequest.getConsumerGroup(), pullRequest.getMessageQueue().getTopic(), pullResult.getMsgFoundList().size());
                            boolean dispatchToConsume = processQueue.putMessage(pullResult.getMsgFoundList());
                            DefaultMQPushConsumerImpl.this.consumeMessageService.submitConsumeRequest(
                                    pullResult.getMsgFoundList(),
                                    processQueue,
                                    pullRequest.getMessageQueue(),
                                    dispatchToConsume);

                            if (DefaultMQPushConsumerImpl.this.defaultMQPushConsumer.getPullInterval() > 0) {
                                DefaultMQPushConsumerImpl.this.executePullRequestLater(pullRequest, DefaultMQPushConsumerImpl.this.defaultMQPushConsumer.getPullInterval());
                            } else {
                                DefaultMQPushConsumerImpl.this.executePullRequestImmediately(pullRequest);
                            }
                        }

                        if (pullResult.getNextBeginOffset() < prevRequestOffset || firstMsgOffset < prevRequestOffset) {
                            log.warn(
                                    "[BUG] pull message result maybe data wrong, nextBeginOffset: {} firstMsgOffset: {} prevRequestOffset: {}",
                                    pullResult.getNextBeginOffset(),
                                    firstMsgOffset,
                                    prevRequestOffset);
                        }

                        break;
                    case NO_NEW_MSG:
                    case NO_MATCHED_MSG:
                        pullRequest.setNextOffset(pullResult.getNextBeginOffset());
                        DefaultMQPushConsumerImpl.this.correctTagsOffset(pullRequest);
                        DefaultMQPushConsumerImpl.this.executePullRequestImmediately(pullRequest);
                        break;
                    case OFFSET_ILLEGAL:
                        log.warn("the pull request offset illegal, {} {}", pullRequest.toString(), pullResult.toString());
                        pullRequest.setNextOffset(pullResult.getNextBeginOffset());

                        pullRequest.getProcessQueue().setDropped(true);
                        DefaultMQPushConsumerImpl.this.executeTaskLater(new Runnable() {

                            @Override
                            public void run() {
                                try {
                                    DefaultMQPushConsumerImpl.this.offsetStore.updateOffset(pullRequest.getMessageQueue(), pullRequest.getNextOffset(), false);
                                    DefaultMQPushConsumerImpl.this.offsetStore.persist(pullRequest.getMessageQueue());
                                    DefaultMQPushConsumerImpl.this.rebalanceImpl.removeProcessQueue(pullRequest.getMessageQueue());
                                    log.warn("fix the pull request offset, {}", pullRequest);
                                } catch (Throwable e) {
                                    log.error("executeTaskLater Exception", e);
                                }
                            }
                        }, 10000);
                        break;
                    default:
                        break;
                }
            }

            @Override
            public void onException(Throwable e) {
                if (!pullRequest.getMessageQueue().getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                    log.warn("execute the pull request exception", e);
                }

                if (e instanceof MQBrokerException && ((MQBrokerException) e).getResponseCode() == ResponseCode.FLOW_CONTROL) {
                    DefaultMQPushConsumerImpl.this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_BROKER_FLOW_CONTROL);
                } else {
                    DefaultMQPushConsumerImpl.this.executePullRequestLater(pullRequest, pullTimeDelayMillsWhenException);
                }
            }
        };
        boolean commitOffsetEnable = false;
        long commitOffsetValue = 0L;
        if (MessageModel.CLUSTERING == this.consumerConfig.getMessageModel()) {
            commitOffsetValue = this.offsetStore.readOffset(pullRequest.getMessageQueue(), ReadOffsetType.READ_FROM_MEMORY);
            if (commitOffsetValue > 0) {
                commitOffsetEnable = true;
            }
        }

        String subExpression = null;
        boolean classFilter = false;
        SubscriptionData sd = this.rebalanceImpl.getSubscriptionInner().get(pullRequest.getMessageQueue().getTopic());
        if (sd != null) {
            if (this.defaultMQPushConsumer.isPostSubscriptionWhenPull() && !sd.isClassFilterMode()) {
                subExpression = sd.getSubString();
            }
            classFilter = sd.isClassFilterMode();
        }

        int sysFlag = PullSysFlag.buildSysFlag(
                commitOffsetEnable, // commitOffset
                true, // suspend
                subExpression != null, // subscription
                classFilter // class filter
        );
        try {
            this.pullAPIWrapper.pullKernelImpl(
                    pullRequest.getMessageQueue(),
                    subExpression,
                    subscriptionData.getExpressionType(),
                    subscriptionData.getSubVersion(),
                    pullRequest.getNextOffset(),
                    this.defaultMQPushConsumer.getPullBatchSize(),
                    sysFlag,
                    commitOffsetValue,
                    BROKER_SUSPEND_MAX_TIME_MILLIS,
                    CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND,
                    CommunicationMode.ASYNC,
                    pullCallback
            );
        } catch (Exception e) {
            log.error("pullKernelImpl exception", e);
            pullMessageLater(pullRequest, this.consumerConfig.getPullTimeDelayMillsWhenException());
        }
    }

    private void pullMessageLater(PullRequest request, long timeout) {
        pullMessageService.later(timeout, () -> pullMessageService.pullMessage(request));
    }

    private boolean checkLimit(PullRequest request, ProcessQueue processQueue) {
        long cachedMessageCount = processQueue.getMsgCount().get();
        if (cachedMessageCount > this.consumerConfig.getPullThresholdForQueue()) {
            pullMessageLater(request, ConsumerConfig.PULL_TIME_DELAY_MILLS_WHEN_CACHE_FLOW_CONTROL);
            log.warn(
                    "pull request later: cached message count limit {}/{}, minOffset={}, maxOffset={}",
                    cachedMessageCount,
                    this.consumerConfig.getPullThresholdForQueue(),
                    processQueue.getMsgTreeMap().firstKey(),
                    processQueue.getMsgTreeMap().lastKey()
            );
            return false;
        }

        long cachedMessageSize = processQueue.getMsgSize().get() / (1024 * 1024);
        if (cachedMessageSize > this.consumerConfig.getPullThresholdSizeForQueue()) {
            pullMessageLater(request, ConsumerConfig.PULL_TIME_DELAY_MILLS_WHEN_CACHE_FLOW_CONTROL);
            log.warn(
                    "pull request later: cached message memory limit {}/{}, minOffset={}, maxOffset={}",
                    cachedMessageSize,
                    this.consumerConfig.getPullThresholdSizeForQueue(),
                    processQueue.getMsgTreeMap().firstKey(),
                    processQueue.getMsgTreeMap().lastKey()
            );
            return false;
        }
        return true;
    }

    private boolean checkServiceState(PullRequest request) {
        if (!isRunning()) {
            log.warn("pull request later: consumer {} not running", this.consumerGroup);
            pullMessageLater(request, ConsumerConfig.PULL_TIME_DELAY_MILLS_WHEN_SUSPEND);
            return false;
        }
        if (isPause()) {
            log.warn("pull request later: consumer {} paused", this.consumerGroup);
            pullMessageLater(request, ConsumerConfig.PULL_TIME_DELAY_MILLS_WHEN_SUSPEND);
            return false;
        }
        return true;
    }

    @Override
    protected void loop() throws Exception {

    }
}
