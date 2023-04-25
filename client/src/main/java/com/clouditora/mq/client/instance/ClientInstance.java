package com.clouditora.mq.client.instance;

import com.clouditora.mq.client.ClientConfig;
import com.clouditora.mq.common.service.AbstractNothingService;
import com.clouditora.mq.network.Client;

/**
 * @link org.apache.rocketmq.client.impl.factory.MQClientInstance
 */
public class ClientInstance extends AbstractNothingService {
    private final Client client;
    private final ClientConfig clientConfig;

    public ClientInstance(Client client, ClientConfig clientConfig) {
        this.client = client;
        this.clientConfig = clientConfig;
    }

    @Override
    public String getServiceName() {
        return "ClientInstance";
    }

    @Override
    public void startup() {
        this.client.updateNameserverEndpoints(this.clientConfig.getNameserverEndpoints());
        super.startup();
    }
}
