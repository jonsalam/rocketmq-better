package com.clouditora.mq.client.broker;

import com.clouditora.mq.common.command.protocol.BrokerRegister;
import com.clouditora.mq.common.command.protocol.BrokerUnregister;
import com.clouditora.mq.network.Client;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class BrokerRpcFacade {
    private final BrokerApiFacade brokerApiFacade;
    private final ExecutorService executor;

    public BrokerRpcFacade(Client client, ExecutorService executor) {
        this.brokerApiFacade = new BrokerApiFacade(client);
        this.executor = executor;
    }

    public void registerBroker(String clusterName, String brokerName, String brokerEndpoint, Long brokerId, long timeout) {
        BrokerRegister.RequestHeader requestHeader = new BrokerRegister.RequestHeader();
        requestHeader.setClusterName(clusterName);
        requestHeader.setBrokerName(brokerName);
        requestHeader.setBrokerEndpoint(brokerEndpoint);
        requestHeader.setBrokerId(brokerId);
        List<String> nameserverEndpoints = this.brokerApiFacade.getClient().getNameserverEndpoints();
        CountDownLatch latch = new CountDownLatch(nameserverEndpoints.size());
        for (String nameserverEndpoint : nameserverEndpoints) {
            executor.submit(() -> {
                try {
                    this.brokerApiFacade.registerBroker(nameserverEndpoint, requestHeader, timeout);
                    log.info("register broker to {}", nameserverEndpoint);
                } catch (Exception e) {
                    log.error("register broker to {} exception", nameserverEndpoint, e);
                }
            });
            latch.countDown();
        }
        try {
            boolean await = latch.await(timeout, TimeUnit.MILLISECONDS);
            log.info("register broker: {}", await);
        } catch (InterruptedException e) {
            log.error("resister broker interrupted", e);
        }
    }

    public void unregisterBroker(String clusterName, String brokerName, String brokerEndpoint, Long brokerId, int timeout) {
        BrokerUnregister.RequestHeader requestHeader = new BrokerUnregister.RequestHeader();
        requestHeader.setClusterName(clusterName);
        requestHeader.setBrokerName(brokerName);
        requestHeader.setBrokerEndpoint(brokerEndpoint);
        requestHeader.setBrokerId(brokerId);
        List<String> nameserverEndpoints = this.brokerApiFacade.getClient().getNameserverEndpoints();
        CountDownLatch latch = new CountDownLatch(nameserverEndpoints.size());
        for (String nameserverEndpoint : nameserverEndpoints) {
            executor.submit(() -> {
                try {
                    this.brokerApiFacade.unregisterBroker(nameserverEndpoint, requestHeader, timeout);
                    log.info("unregister broker to {}", nameserverEndpoint);
                } catch (Exception e) {
                    log.error("unregister broker to {} exception", nameserverEndpoint, e);
                }
            });
            latch.countDown();
        }
        try {
            boolean await = latch.await(timeout, TimeUnit.MILLISECONDS);
            log.info("unregister broker: {}", await);
        } catch (InterruptedException e) {
            log.error("unregister broker interrupted", e);
        }
    }
}
