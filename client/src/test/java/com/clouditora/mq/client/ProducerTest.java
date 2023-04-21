package com.clouditora.mq.client;

import com.clouditora.mq.client.exception.MqClientException;
import com.clouditora.mq.client.producer.DefaultProducer;
import com.clouditora.mq.client.producer.SendResult;
import com.clouditora.mq.client.producer.SendStatus;
import com.clouditora.mq.common.Message;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class ProducerTest {

    @Test
    void producer() throws MqClientException, InterruptedException {
        Producer producer = new DefaultProducer("group-junit");
        producer.setNameserver("127.0.0.1:9876");
        producer.startup();

        Message msg = new Message("foo", "bar", "bar", "Hello RocketMQ".getBytes(StandardCharsets.UTF_8));
        SendResult sendResult = producer.send(msg);
        assertEquals(SendStatus.SEND_OK, sendResult.getSendStatus());
        producer.shutdown();
    }

}