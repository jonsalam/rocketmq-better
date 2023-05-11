package com.clouditora.mq.broker;

import com.clouditora.mq.broker.listener.BrokerChannelListener;
import com.clouditora.mq.broker.nameserver.NameserverApiFacade;
import com.clouditora.mq.broker.processor.DefaultRequestProcessor;
import com.clouditora.mq.common.service.AbstractScheduledService;
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
public class BrokerController extends AbstractScheduledService {
    private final BrokerConfig brokerConfig;
    private final ServerNetworkConfig serverNetworkConfig;
    private final ClientNetworkConfig clientNetworkConfig;
    private final Server server;
    private final Client client;
    private final ExecutorService nameserverRpcExecutor;
    private final NameserverApiFacade nameserverApiFacade;

    public BrokerController(BrokerConfig brokerConfig, ServerNetworkConfig serverNetworkConfig, ClientNetworkConfig clientNetworkConfig) {
        this.brokerConfig = brokerConfig;
        this.serverNetworkConfig = serverNetworkConfig;
        this.clientNetworkConfig = clientNetworkConfig;
        this.server = new Server(this.serverNetworkConfig, new BrokerChannelListener());
        DefaultRequestProcessor requestProcessor = new DefaultRequestProcessor(this);
        this.server.setDefaultProcessor(requestProcessor, null);
        this.client = new Client(clientNetworkConfig, null);
        this.client.updateNameserverEndpoints(this.brokerConfig.getNameserverEndpoints());
        this.nameserverRpcExecutor = new ThreadPoolExecutor(
                4, 10,
                1, TimeUnit.MINUTES,
                new ArrayBlockingQueue<>(32),
                ThreadUtil.buildFactory("RpcExecutor", 10)
        );
        this.nameserverApiFacade = new NameserverApiFacade(client, nameserverRpcExecutor);
    }

    @Override
    public String getServiceName() {
        return "Broker";
    }

    @Override
    public void startup() {
        this.server.startup();
        this.client.startup();
        registerBroker();
        // @link org.apache.rocketmq.common.BrokerConfig#registerNameServerPeriod
        scheduled(TimeUnit.SECONDS, 10, 30, this::registerBroker);
        super.startup();
    }

    @Override
    public void shutdown() {
        this.server.shutdown();
        this.client.shutdown();
        this.nameserverRpcExecutor.shutdown();
        unregisterBroker();
        super.shutdown();
    }

    /**
     * @link org.apache.rocketmq.broker.BrokerController#registerBrokerAll
     */
    private void registerBroker() {
        String endpoint = this.brokerConfig.getBrokerIp() + ":" + serverNetworkConfig.getListenPort();
        this.nameserverApiFacade.registerBroker(
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
        this.nameserverApiFacade.unregisterBroker(
                this.brokerConfig.getBrokerClusterName(),
                endpoint,
                endpoint,
                this.brokerConfig.getBrokerId(),
                this.brokerConfig.getRegisterBrokerTimeoutMills()
        );
    }
}
