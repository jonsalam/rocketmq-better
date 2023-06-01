package com.clouditora.mq.client.broker;

import com.clouditora.mq.client.consumer.pull.PullMessageRequest;
import com.clouditora.mq.client.instance.ClientConfig;
import com.clouditora.mq.client.producer.SendResult;
import com.clouditora.mq.common.Message;
import com.clouditora.mq.common.broker.BrokerEndpoints;
import com.clouditora.mq.common.constant.GlobalConstant;
import com.clouditora.mq.common.constant.RpcModel;
import com.clouditora.mq.common.exception.BrokerException;
import com.clouditora.mq.common.topic.ConsumerSubscription;
import com.clouditora.mq.common.topic.ConsumerSubscriptions;
import com.clouditora.mq.common.topic.ProducerGroup;
import com.clouditora.mq.common.topic.TopicQueue;
import com.clouditora.mq.network.exception.ConnectException;
import com.clouditora.mq.network.exception.TimeoutException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;

import java.util.List;
import java.util.Map;
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

    public SendResult sendMessage(RpcModel rpcModel, String group, TopicQueue queue, Message message, long timeout) throws BrokerException, InterruptedException, ConnectException, TimeoutException {
        BrokerEndpoints endpoints = this.endpointMap.get(queue.getBrokerName());
        String endpoint = endpoints.getEndpointMap().get(GlobalConstant.MASTER_ID);
        return this.brokerApiFacade.sendMessage(
                rpcModel,
                queue.getBrokerName(), endpoint,
                group,
                message,
                queue.getQueueId(),
                timeout
        );
    }

    /**
     * @link org.apache.rocketmq.client.impl.factory.MQClientInstance#findBrokerAddressInSubscribe
     */
    public String findEndpoint(String brokerName, long brokerId, boolean onlyThisBroker) {
        BrokerEndpoints endpoints = this.endpointMap.get(brokerName);
        if (endpoints == null || endpoints.isEmpty()) {
            return null;
        }
        String endpoint = endpoints.get(brokerId);
        if (endpoint == null && brokerId != GlobalConstant.MASTER_ID) {
            endpoint = endpoints.get(brokerId + 1);
        }
        if (endpoint == null && !onlyThisBroker) {
            Map.Entry<Long, String> entry = endpoints.getEndpointMap().entrySet().iterator().next();
            endpoint = entry.getValue();
        }
        return endpoint;
    }

    public void pullMessage(PullMessageRequest request, ConsumerSubscription subscription, long offset, int pullBatchSize) throws BrokerException, InterruptedException, ConnectException, TimeoutException {
        String endpoint = findEndpoint(brokerName, brokerId, false);
        this.brokerApiFacade.pullMessage(
                RpcModel.ASYNC,
                endpoint
        );
    }
}
