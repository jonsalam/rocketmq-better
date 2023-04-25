package com.clouditora.mq.client.producer;

import com.clouditora.mq.client.ProducerConfig;
import com.clouditora.mq.common.service.AbstractNothingService;
import com.clouditora.mq.common.util.ThreadUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.*;

/**
 * @link org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl
 */
@Slf4j
public class DefaultMqProducer extends AbstractNothingService {
    protected ProducerConfig producerConfig;
    private final BlockingQueue<Runnable> asyncSenderQueue;
    private final ExecutorService asyncSenderExecutor;

    public DefaultMqProducer(String producerGroup) {
        super();
        this.producerConfig = new ProducerConfig();
        this.producerConfig.setProducerGroup(producerGroup);

        this.asyncSenderQueue = new LinkedBlockingQueue<>(50000);
        int poolSize = Runtime.getRuntime().availableProcessors();
        this.asyncSenderExecutor = new ThreadPoolExecutor(
                poolSize,
                poolSize,
                1000 * 60,
                TimeUnit.MILLISECONDS,
                this.asyncSenderQueue,
                ThreadUtil.buildFactory("AsyncSenderExecutor", poolSize)
        );
    }

    @Override
    public String getServiceName() {
        return "Producer";
    }

}
