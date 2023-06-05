package com.clouditora.mq.client.consumer.listener;

import com.clouditora.mq.client.consumer.handler.ConsumeStatus;
import com.clouditora.mq.common.Message;

/**
 * @link org.apache.rocketmq.client.consumer.listener.MessageListener
 * @link org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently
 */
public interface MessageListener {
    /**
     * @link org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently#consumeMessage
     */
    ConsumeStatus consume(Message message);
}
