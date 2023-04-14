package com.clouditora.mq.client.consumer;

import com.clouditora.mq.common.service.AbstractLaterService;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * @link org.apache.rocketmq.client.impl.consumer.PullMessageService
 */
@Slf4j
public
class PullMessageService extends AbstractLaterService {
    private final LinkedBlockingQueue<PullRequest> pullRequestQueue;
    private final ConsumerManager consumerManager;

    public PullMessageService(ConsumerManager consumerManager) {
        this.pullRequestQueue = new LinkedBlockingQueue<>();
        this.consumerManager = consumerManager;
    }

    @Override
    public String getServiceName() {
        return "PullMessage";
    }

    @Override
    protected void loop() throws Exception {
        PullRequest pullRequest = this.pullRequestQueue.take();
        pullMessage(pullRequest);
    }

    public void pullMessage(PullRequest request) {
        DefaultMqConsumer consumer = this.consumerManager.selectConsumer(request.getConsumerGroup());
        if (consumer == null) {
            log.error("{} no matched consumer: {}", getServiceName(), request.getConsumerGroup());
        } else {
            consumer.pullMessage(request);
        }
    }
}