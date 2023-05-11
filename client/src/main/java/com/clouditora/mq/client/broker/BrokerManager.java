package com.clouditora.mq.client.broker;

import com.clouditora.mq.client.instance.ClientConfig;
import com.clouditora.mq.common.command.protocol.ClientHeartBeatCommand;
import com.clouditora.mq.common.route.BrokerEndpoint;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
public class BrokerManager {
    private ClientConfig clientConfig;
    /**
     * name: [id: endpoint]
     *
     * @link org.apache.rocketmq.client.impl.factory.MQClientInstance#brokerAddrTable
     */
    private final ConcurrentMap<String, BrokerEndpoint> brokerEndpointMap = new ConcurrentHashMap<>();
    private final BrokerApiFacade brokerApiFacade;
    /**
     * @link org.apache.rocketmq.client.impl.factory.MQClientInstance#lockHeartbeat
     */
    private final Lock lock = new ReentrantLock();

    public BrokerManager(BrokerApiFacade brokerApiFacade) {
        this.brokerApiFacade = brokerApiFacade;
    }

    public void addBrokers(List<BrokerEndpoint> brokers) {
        for (BrokerEndpoint endpoint : brokers) {
            this.brokerEndpointMap.put(endpoint.getName(), endpoint);
        }
    }

    /**
     * @link org.apache.rocketmq.client.impl.factory.MQClientInstance#sendHeartbeatToAllBrokerWithLock
     */
    public void heartbeat(ClientHeartBeatCommand.RequestBody heartBeat) {
        if (this.lock.tryLock()) {
            try {
                heartbeat0(heartBeat);
            } catch (final Exception e) {
                log.error("send heartbeat exception", e);
            } finally {
                this.lock.unlock();
            }
        } else {
            log.warn("lock heartBeat, but failed");
        }
    }

    /**
     * @link org.apache.rocketmq.client.impl.factory.MQClientInstance#sendHeartbeatToAllBroker
     */
    private void heartbeat0(ClientHeartBeatCommand.RequestBody heartBeat) {
        if (MapUtils.isEmpty(this.brokerEndpointMap)) {
            log.warn("heartbeat: broker endpoint is empty");
            return;
        }
        for (BrokerEndpoint endpoints : brokerEndpointMap.values()) {
            for (String endpoint : endpoints.getEndpointMap().values()) {
                try {
                    brokerApiFacade.heartbeat(endpoint, heartBeat, clientConfig.getMqClientApiTimeout());
                } catch (Exception e) {
                    log.error("heartbeat exception", e);
                }
            }
        }
    }

    public void unregisterClient(String group) {

    }
}
