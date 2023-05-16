package com.clouditora.mq.client.broker;

import com.clouditora.mq.client.instance.ClientConfig;
import com.clouditora.mq.common.broker.BrokerEndpoints;
import com.clouditora.mq.common.constant.GlobalConstant;
import com.clouditora.mq.common.topic.ConsumerSubscriptions;
import com.clouditora.mq.common.topic.ProducerGroup;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Slf4j
public class BrokerManager {
    private ClientConfig clientConfig;
    /**
     * name: [id: endpoint]
     *
     * @link org.apache.rocketmq.client.impl.factory.MQClientInstance#brokerAddrTable
     */
    private final ConcurrentMap<String, BrokerEndpoints> endpointMap = new ConcurrentHashMap<>();
    private final BrokerApiFacade brokerApiFacade;

    public BrokerManager(BrokerApiFacade brokerApiFacade) {
        this.brokerApiFacade = brokerApiFacade;
    }

    public void addBrokers(List<BrokerEndpoints> brokers) {
        if (CollectionUtils.isEmpty(brokers)) {
            return;
        }
        for (BrokerEndpoints endpoint : brokers) {
            this.endpointMap.put(endpoint.getBrokerName(), endpoint);
        }
    }

    /**
     * @link org.apache.rocketmq.client.impl.factory.MQClientInstance#sendHeartbeatToAllBrokerWithLock
     * @link org.apache.rocketmq.client.impl.factory.MQClientInstance#sendHeartbeatToAllBroker
     */
    public void heartbeat(String clientId, Set<ProducerGroup> producers, Set<ConsumerSubscriptions> consumers) {
        if (MapUtils.isEmpty(this.endpointMap)) {
            log.warn("heartbeat: broker endpoint is empty");
            return;
        }
        for (BrokerEndpoints endpoints : endpointMap.values()) {
            String endpoint = endpoints.getEndpointMap().get(GlobalConstant.MASTER_ID);
            if (endpoint != null) {
                try {
                    this.brokerApiFacade.heartbeat(endpoint, clientId, producers, consumers);
                } catch (Exception e) {
                    log.error("heartbeat exception", e);
                }
            }
        }
    }

    /**
     * @link org.apache.rocketmq.client.impl.factory.MQClientInstance#unregisterClient
     */
    public void unregisterClient(String clientId, String producerGroup, String consumerGroup) {
        if (MapUtils.isEmpty(this.endpointMap)) {
            log.warn("unregister client: broker endpoint is empty");
            return;
        }
        for (BrokerEndpoints endpoints : endpointMap.values()) {
            for (String endpoint : endpoints.getEndpointMap().values()) {
                try {
                    this.brokerApiFacade.unregisterClient(endpoint, clientId, producerGroup, consumerGroup);
                } catch (Exception e) {
                    log.error("heartbeat exception", e);
                }
            }
        }
    }
}
