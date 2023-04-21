package com.clouditora.mq.broker.nameserver;

import com.clouditora.mq.broker.BrokerConfig;
import com.clouditora.mq.common.constant.RpcModel;
import com.clouditora.mq.common.network.command.BrokerRegisterCommand;
import com.clouditora.mq.common.network.command.BrokerUnregisterCommand;
import com.clouditora.mq.common.topic.TopicQueue;
import com.clouditora.mq.network.ClientNetwork;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @link org.apache.rocketmq.broker.out.BrokerOuterAPI
 */
@Slf4j
public class NameserverApiFacade {
    private final BrokerConfig brokerConfig;
    private final NameserverApi nameserverApi;
    private final ExecutorService executor;

    public NameserverApiFacade(BrokerConfig brokerConfig, ClientNetwork clientNetwork, ExecutorService executor) {
        this.brokerConfig = brokerConfig;
        this.nameserverApi = new NameserverApi(clientNetwork);
        this.executor = executor;
    }

    /**
     * @link org.apache.rocketmq.broker.out.BrokerOuterAPI#registerBrokerAll
     */
    public void registerBroker(RpcModel rpcModel, String clusterName, String brokerName, String brokerEndpoint, Long brokerId, ConcurrentMap<String, TopicQueue> topicMap) {
        BrokerRegisterCommand.RequestHeader requestHeader = new BrokerRegisterCommand.RequestHeader();
        requestHeader.setClusterName(clusterName);
        requestHeader.setBrokerName(brokerName);
        requestHeader.setBrokerEndpoint(brokerEndpoint);
        requestHeader.setBrokerId(brokerId);
        BrokerRegisterCommand.RequestBody requestBody = new BrokerRegisterCommand.RequestBody();
        requestBody.setTopicMap(topicMap);

        List<String> nameserverEndpoints = this.nameserverApi.getClientNetwork().getNameserverEndpoints();
        CountDownLatch countDownLatch = new CountDownLatch(nameserverEndpoints.size());
        for (String endpoint : nameserverEndpoints) {
            executor.submit(() -> {
                try {
                    this.nameserverApi.registerBroker(rpcModel, endpoint, requestHeader, requestBody, this.brokerConfig.getRegisterBrokerTimeoutMills());
                    log.info("register broker to {}", endpoint);
                } catch (Exception e) {
                    log.error("register broker to {} exception", endpoint, e);
                }
            });
            countDownLatch.countDown();
        }
        try {
            boolean await = countDownLatch.await(this.brokerConfig.getRegisterBrokerTimeoutMills(), TimeUnit.MILLISECONDS);
            log.info("register broker: {}", await);
        } catch (InterruptedException e) {
            log.error("resister broker interrupted", e);
        }
    }

    /**
     * @link org.apache.rocketmq.broker.out.BrokerOuterAPI#unregisterBrokerAll
     */
    public void unregisterBroker(String clusterName, String brokerName, String brokerEndpoint, Long brokerId) {
        BrokerUnregisterCommand.RequestHeader requestHeader = new BrokerUnregisterCommand.RequestHeader();
        requestHeader.setClusterName(clusterName);
        requestHeader.setBrokerName(brokerName);
        requestHeader.setBrokerId(brokerId);
        requestHeader.setBrokerEndpoint(brokerEndpoint);
        List<String> nameserverEndpoints = this.nameserverApi.getClientNetwork().getNameserverEndpoints();
        CountDownLatch latch = new CountDownLatch(nameserverEndpoints.size());
        for (String endpoint : nameserverEndpoints) {
            executor.submit(() -> {
                try {
                    this.nameserverApi.unregisterBroker(endpoint, requestHeader, this.brokerConfig.getRegisterBrokerTimeoutMills());
                    log.info("unregister broker to {}", endpoint);
                } catch (Exception e) {
                    log.error("unregister broker to {} exception", endpoint, e);
                }
            });
            latch.countDown();
        }
        try {
            boolean await = latch.await(this.brokerConfig.getRegisterBrokerTimeoutMills(), TimeUnit.MILLISECONDS);
            log.info("unregister broker: {}", await);
        } catch (InterruptedException e) {
            log.error("unregister broker interrupted", e);
        }
    }
}
