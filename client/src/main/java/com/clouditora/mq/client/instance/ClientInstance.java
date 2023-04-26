package com.clouditora.mq.client.instance;

import com.clouditora.mq.client.ClientConfig;
import com.clouditora.mq.common.service.AbstractNothingService;
import com.clouditora.mq.network.Client;
import com.clouditora.mq.network.ClientNetworkConfig;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @link org.apache.rocketmq.client.impl.factory.MQClientInstance
 */
@Slf4j
public class ClientInstance extends AbstractNothingService {
    private final ClientConfig clientConfig;
    private final Client client;
    /**
     * @link org.apache.rocketmq.client.impl.factory.MQClientInstance#lockHeartbeat
     */
    private final Lock lockHeartbeat = new ReentrantLock();

    public ClientInstance(ClientConfig clientConfig) {
        this.clientConfig = clientConfig;
        ClientNetworkConfig networkConfig = new ClientNetworkConfig();
        networkConfig.setClientCallbackExecutorThreads(clientConfig.getClientCallbackExecutorThreads());
        this.client = new Client(networkConfig, null);
    }

    @Override
    public String getServiceName() {
        return "ClientInstance#" + this.clientConfig.getInstanceName();
    }

    @Override
    public void startup() {
        this.client.updateNameserverEndpoints(this.clientConfig.getNameserverEndpoints());
        this.client.startup();
        super.startup();
    }

    public void registerProducer(){

    }

    /**
     * @link org.apache.rocketmq.client.impl.factory.MQClientInstance#sendHeartbeatToAllBrokerWithLock
     */
    private void sendHeartbeatToAllBrokerWithLock() {
        if (this.lockHeartbeat.tryLock()) {
            try {
                this.sendHeartbeatToAllBroker();
                this.uploadFilterClassSource();
            } catch (final Exception e) {
                log.error("sendHeartbeatToAllBroker exception", e);
            } finally {
                this.lockHeartbeat.unlock();
            }
        } else {
            log.warn("lock heartBeat, but failed. [{}]", this.clientId);
        }
    }
}
