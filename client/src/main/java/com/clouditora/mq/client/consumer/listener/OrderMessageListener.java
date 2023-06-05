package com.clouditora.mq.client.consumer.listener;

import com.clouditora.mq.client.consumer.handler.ConsumeStatus;
import com.clouditora.mq.common.Message;

public class OrderMessageListener implements MessageListener{
    @Override
    public ConsumeStatus consume(Message message) {
        return null;
    }
}
