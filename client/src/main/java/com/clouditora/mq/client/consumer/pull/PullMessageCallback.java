package com.clouditora.mq.client.consumer.pull;

/**
 * @link org.apache.rocketmq.client.consumer.PullCallback
 */
public interface PullMessageCallback {
    void onSuccess(PullResult pullResult);

    void onException(Exception e);
}
