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
public class NameServerRpcFacade {
    private final NameServerApiFacade nameServerApiFacade;
    private final ExecutorService executor;

    public NameServerRpcFacade(Client client, ExecutorService executor) {
        this.nameServerApiFacade = new NameServerApiFacade(client);
        this.executor = executor;
    }

    public void registerBroker(String clusterName, String brokerName, String brokerEndpoint, Long brokerId, long timeout) {
        BrokerRegister.RequestHeader requestHeader = new BrokerRegister.RequestHeader();
        requestHeader.setClusterName(clusterName);
        requestHeader.setBrokerName(brokerName);
        requestHeader.setBrokerEndpoint(brokerEndpoint);
        requestHeader.setBrokerId(brokerId);
        List<String> nameServerEndpoints = this.nameServerApiFacade.getClient().getNameServerEndpoints();
        CountDownLatch latch = new CountDownLatch(nameServerEndpoints.size());
        for (String nameServerEndpoint : nameServerEndpoints) {
            executor.submit(() -> {
                try {
                    this.nameServerApiFacade.registerBroker(nameServerEndpoint, requestHeader, timeout);
                    log.info("register broker to {}", nameServerEndpoint);
                } catch (Exception e) {
                    log.error("register broker to {} exception", nameServerEndpoint, e);
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
        List<String> nameServerEndpoints = this.nameServerApiFacade.getClient().getNameServerEndpoints();
        CountDownLatch latch = new CountDownLatch(nameServerEndpoints.size());
        for (String nameServerEndpoint : nameServerEndpoints) {
            executor.submit(() -> {
                try {
                    this.nameServerApiFacade.unregisterBroker(nameServerEndpoint, requestHeader, timeout);
                    log.info("unregister broker to {}", nameServerEndpoint);
                } catch (Exception e) {
                    log.error("unregister broker to {} exception", nameServerEndpoint, e);
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
