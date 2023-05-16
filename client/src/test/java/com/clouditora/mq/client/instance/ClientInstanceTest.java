package com.clouditora.mq.client.instance;

import com.clouditora.mq.client.producer.Producer;
import org.junit.jupiter.api.Test;

class ClientInstanceTest {

    @Test
    void heartbeat() {
        ClientConfig config = new ClientConfig();
        Producer producer = new Producer("test");
        ClientInstance instance = new ClientInstance(config);
        instance.registerProducer(producer);
        instance.startup();
//        instance.heartbeat();
    }
}