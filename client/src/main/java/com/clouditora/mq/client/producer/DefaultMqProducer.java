package com.clouditora.mq.client.producer;

import com.clouditora.mq.client.ClientConfig;
import com.clouditora.mq.client.instance.ClientInstance;
import com.clouditora.mq.client.instance.ClientInstanceFactory;
import com.clouditora.mq.common.constant.GlobalConstant;
import com.clouditora.mq.common.service.AbstractNothingService;
import com.clouditora.mq.network.ClientNetworkConfig;
import lombok.extern.slf4j.Slf4j;

/**
 * @link org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl
 */
@Slf4j
public class DefaultMqProducer extends AbstractNothingService {
    protected final ClientConfig clientConfig;
    protected final ClientNetworkConfig networkConfig;
    protected final ProducerConfig producerConfig;
    protected final ClientInstance clientInstance;

    public DefaultMqProducer(String producerGroup, ClientConfig clientConfig, ClientNetworkConfig networkConfig) {
        super();
        String instanceName = clientConfig.getInstanceName();
        if (ClientConfig.DEFAULT_INSTANCE.equals(instanceName)) {
            // @link org.apache.rocketmq.client.ClientConfig#changeInstanceNameToPID
            clientConfig.setInstanceName(String.format("%s#%d", GlobalConstant.PID, System.nanoTime()));
        }
        this.clientConfig = clientConfig;
        this.networkConfig = networkConfig;
        this.producerConfig = new ProducerConfig();
        this.producerConfig.setProducerGroup(producerGroup);

        this.clientInstance = ClientInstanceFactory.create(clientConfig);
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
