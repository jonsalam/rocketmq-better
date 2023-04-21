package com.clouditora.mq.broker;

import com.clouditora.mq.broker.listener.ChannelListener;
import com.clouditora.mq.broker.nameserver.NameServerRpcFacade;
import com.clouditora.mq.common.service.AbstractNothingService;
import com.clouditora.mq.common.util.ThreadUtil;
import com.clouditora.mq.network.Client;
import com.clouditora.mq.network.ClientNetworkConfig;
import com.clouditora.mq.network.Server;
import com.clouditora.mq.network.ServerNetworkConfig;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Slf4j
public class BrokerController extends AbstractNothingService {
    private final BrokerConfig brokerConfig;
    private final ServerNetworkConfig serverNetworkConfig;
    private final ClientNetworkConfig clientNetworkConfig;
    private final Server server;
    private final Client client;
    private final NameServerRpcFacade nameServerRpcFacade;
    private final ExecutorService nameServerRpcExecutor;

    public BrokerController(BrokerConfig brokerConfig, ServerNetworkConfig serverNetworkConfig, ClientNetworkConfig clientNetworkConfig) {
        this.brokerConfig = brokerConfig;
        this.serverNetworkConfig = serverNetworkConfig;
        this.clientNetworkConfig = clientNetworkConfig;
        this.server = new Server(this.serverNetworkConfig, new ChannelListener());
        this.client = new Client(clientNetworkConfig, null, BrokerController.this::registerBroker);
        this.nameServerRpcExecutor = new ThreadPoolExecutor(
                4, 10,
                1, TimeUnit.MINUTES,
                new ArrayBlockingQueue<>(32),
                ThreadUtil.buildFactory("RpcExecutor", 10)
        );
        this.nameServerRpcFacade = new NameServerRpcFacade(client, nameServerRpcExecutor);
    }

    @Override
    public String getServiceName() {
        return "Broker";
    }

    @Override
    public void startup() {
        this.client.updateNameServerEndpoints(this.brokerConfig.getNameServerEndpoints());
        this.server.startup();
        this.client.startup();
        registerBroker();
        super.startup();
    }

    @Override
    public void shutdown() {
        this.server.shutdown();
        this.client.shutdown();
        this.nameServerRpcExecutor.shutdown();
        unregisterBroker();
        super.shutdown();
    }

    /**
     * @link org.apache.rocketmq.broker.BrokerController#registerBrokerAll
     */
    private void registerBroker() {
        String endpoint = this.brokerConfig.getBrokerIp() + ":" + serverNetworkConfig.getListenPort();
        this.nameServerRpcFacade.registerBroker(
                this.brokerConfig.getBrokerClusterName(),
                endpoint,
                endpoint,
                this.brokerConfig.getBrokerId(),
                this.brokerConfig.getRegisterBrokerTimeoutMills()
        );
    }

    /**
     * @link org.apache.rocketmq.broker.BrokerController#unregisterBrokerAll
     */
    private void unregisterBroker() {
        String endpoint = this.brokerConfig.getBrokerIp() + ":" + serverNetworkConfig.getListenPort();
        this.nameServerRpcFacade.unregisterBroker(
                this.brokerConfig.getBrokerClusterName(),
                endpoint,
                endpoint,
                this.brokerConfig.getBrokerId(),
                this.brokerConfig.getRegisterBrokerTimeoutMills()
        );
    }
}
