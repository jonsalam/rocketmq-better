package com.clouditora.mq.broker.nameserver;

import com.clouditora.mq.common.command.protocol.BrokerRegisterCommand;
import com.clouditora.mq.common.command.protocol.BrokerUnregisterCommand;
import com.clouditora.mq.network.Client;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class NameserverApiFacade {
    private final NameserverApi nameserverApi;
    private final ExecutorService executor;

    public NameserverApiFacade(Client client, ExecutorService executor) {
        this.nameserverApi = new NameserverApi(client);
        this.executor = executor;
    }

    public void registerBroker(String clusterName, String brokerName, String brokerEndpoint, Long brokerId, long timeout) {
        BrokerRegisterCommand.RequestHeader requestHeader = new BrokerRegisterCommand.RequestHeader();
        requestHeader.setClusterName(clusterName);
        requestHeader.setBrokerName(brokerName);
        requestHeader.setBrokerEndpoint(brokerEndpoint);
        requestHeader.setBrokerId(brokerId);
        List<String> nameserverEndpoints = this.nameserverApi.getClient().getNameserverEndpoints();
        CountDownLatch latch = new CountDownLatch(nameserverEndpoints.size());
        for (String nameserverEndpoint : nameserverEndpoints) {
            executor.submit(() -> {
                try {
                    this.nameserverApi.registerBroker(nameserverEndpoint, requestHeader, timeout);
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
        BrokerUnregisterCommand.RequestHeader requestHeader = new BrokerUnregisterCommand.RequestHeader();
        requestHeader.setClusterName(clusterName);
        requestHeader.setBrokerName(brokerName);
        requestHeader.setBrokerEndpoint(brokerEndpoint);
        requestHeader.setBrokerId(brokerId);
        List<String> nameserverEndpoints = this.nameserverApi.getClient().getNameserverEndpoints();
        CountDownLatch latch = new CountDownLatch(nameserverEndpoints.size());
        for (String nameserverEndpoint : nameserverEndpoints) {
            executor.submit(() -> {
                try {
                    this.nameserverApi.unregisterBroker(nameserverEndpoint, requestHeader, timeout);
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
