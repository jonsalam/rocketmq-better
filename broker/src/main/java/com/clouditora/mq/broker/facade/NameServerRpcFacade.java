package com.clouditora.mq.broker.facade;

import com.clouditora.mq.common.command.BrokerRegister;
import com.clouditora.mq.common.service.AbstractNothingService;
import com.clouditora.mq.common.util.ThreadUtil;
import com.clouditora.mq.network.Client;
import com.clouditora.mq.network.ClientNetworkConfig;
import com.clouditora.mq.network.client.ClientNameServerHolder;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.*;

@Slf4j
public class NameServerRpcFacade extends AbstractNothingService{
    private final Client client;
    private final NameServerApiFacade nameServerApiFacade;
    private final ExecutorService executor;

    public NameServerRpcFacade(ClientNetworkConfig networkConfig) {
        this.client = new Client(networkConfig, null);
        this.executor = new ThreadPoolExecutor(
                4, 10,
                1, TimeUnit.MINUTES,
                new ArrayBlockingQueue<>(32),
                ThreadUtil.buildFactory("RpcExecutor", 10)
        );
        this.nameServerApiFacade = new NameServerApiFacade(this.client);
    }

    @Override
    public String getServiceName() {
        return "NameServerRpc";
    }

    @Override
    public void startup() {
        this.client.startup();
        super.startup();
    }

    @Override
    public void shutdown() {
        this.client.shutdown();
        super.shutdown();
    }

    public void registerBroker(String clusterName, String brokerName, String brokerAddress, Long brokerId, long timeout) {
        ClientNameServerHolder nameServerHolder = this.client.getNameServerHolder();
        List<String> nameServerAddressList = nameServerHolder.getNameServerAddressList();
        BrokerRegister.RequestHeader requestHeader = new BrokerRegister.RequestHeader();
        requestHeader.setClusterName(clusterName);
        requestHeader.setBrokerName(brokerName);
        requestHeader.setBrokerAddr(brokerAddress);
        requestHeader.setBrokerId(brokerId);
        CountDownLatch latch = new CountDownLatch(nameServerAddressList.size());
        for (String address : nameServerAddressList) {
            executor.submit(() -> {
                try {
                    this.nameServerApiFacade.registerBroker(address, requestHeader, timeout);
                } catch (Exception e) {
                    log.error("register broker exception", e);
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
}
