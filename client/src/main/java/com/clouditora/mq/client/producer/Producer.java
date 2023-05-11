package com.clouditora.mq.client.producer;

import com.clouditora.mq.common.service.AbstractNothingService;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * @link org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl
 */
@Slf4j
public class Producer extends AbstractNothingService {
    @Getter
    private String group;
    protected final ProducerConfig producerConfig;

    public Producer(String group) {
        super();
        this.group = group;
        this.producerConfig = new ProducerConfig();
        this.producerConfig.setProducerGroup(group);
    }

    @Override
    public String getServiceName() {
        return "Producer";
    }

    @Override
    public void startup() {
        super.startup();
    }
}
