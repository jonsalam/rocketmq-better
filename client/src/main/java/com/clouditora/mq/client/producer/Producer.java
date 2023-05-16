package com.clouditora.mq.client.producer;

import com.clouditora.mq.client.instance.ClientConfig;
import com.clouditora.mq.client.instance.ClientInstance;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * @link org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl
 */
@Slf4j
public class Producer {
    @Getter
    private String group;
    @Getter
    private String topic;
    protected final ProducerConfig producerConfig;

    public Producer(String group) {
        super();
        this.group = group;
        this.topic = topic;
        this.producerConfig = new ProducerConfig();
        this.producerConfig.setProducerGroup(group);
    }

    public static void main(String[] args) {
        ClientConfig config = new ClientConfig();
        Producer producer = new Producer("test");
        ClientInstance instance = new ClientInstance(config);
        instance.registerProducer(producer);
        instance.startup();
//        instance.heartbeat();
    }
}
