package com.clouditora.mq.broker.nameserver;

import com.clouditora.mq.common.command.protocol.BrokerRegister;
import com.clouditora.mq.common.command.protocol.BrokerUnregister;
import com.clouditora.mq.network.Client;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class NameserverRpcFacade {
    private final NameserverApiFacade nameserverApiFacade;
    private final ExecutorService executor;

    public NameserverRpcFacade(Client client, ExecutorService executor) {
        this.nameserverApiFacade = new NameserverApiFacade(client);
        this.executor = executor;
    }

    public void registerBroker(String clusterName, String brokerName, String brokerEndpoint, Long brokerId, long timeout) {
        BrokerRegister.RequestHeader requestHeader = new BrokerRegister.RequestHeader();
        requestHeader.setClusterName(clusterName);
        requestHeader.setBrokerName(brokerName);
        requestHeader.setBrokerEndpoint(brokerEndpoint);
        requestHeader.setBrokerId(brokerId);
        List<String> nameserverEndpoints = this.nameserverApiFacade.getClient().getNameserverEndpoints();
        CountDownLatch latch = new CountDownLatch(nameserverEndpoints.size());
        for (String nameserverEndpoint : nameserverEndpoints) {
            executor.submit(() -> {
                try {
                    this.nameserverApiFacade.registerBroker(nameserverEndpoint, requestHeader, timeout);
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
        List<String> nameserverEndpoints = this.nameserverApiFacade.getClient().getNameserverEndpoints();
        CountDownLatch latch = new CountDownLatch(nameserverEndpoints.size());
        for (String nameserverEndpoint : nameserverEndpoints) {
            executor.submit(() -> {
                try {
                    this.nameserverApiFacade.unregisterBroker(nameserverEndpoint, requestHeader, timeout);
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
