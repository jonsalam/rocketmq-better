package com.clouditora.mq.client;

import com.clouditora.mq.client.consumer.DefaultConsumer;
import com.clouditora.mq.client.consumer.listener.ConsumeStatus;
import com.clouditora.mq.client.consumer.listener.MessageListenerConcurrently;
import com.clouditora.mq.common.Message;
import com.clouditora.mq.common.constant.MessageModel;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
public class ConsumerTest {

    @Test
    void test() {
        DefaultConsumer consumer = new DefaultConsumer("group-junit");
        consumer.setInstanceName("test");
        consumer.setNameserver("127.0.0.1:9876");
        consumer.setSubscribe("foo", "*");
        consumer.setMessageModel(MessageModel.CLUSTERING);
        consumer.registerMessageListener((MessageListenerConcurrently) messages -> {
            for (Message message : messages) {
                log.info("received: {}", message);
            }
            return ConsumeStatus.SUCCESS;
        });
        consumer.startup();
    }

}
