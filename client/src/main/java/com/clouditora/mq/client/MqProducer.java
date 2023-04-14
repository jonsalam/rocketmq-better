package com.clouditora.mq.client;

import com.clouditora.mq.client.exception.MqClientException;
import com.clouditora.mq.client.producer.SendResult;
import com.clouditora.mq.common.Message;
import com.clouditora.mq.common.service.Lifecycle;

public interface MqProducer extends Lifecycle {
    void setNameServer(String address);

    SendResult send(Message message) throws MqClientException, InterruptedException;
}
