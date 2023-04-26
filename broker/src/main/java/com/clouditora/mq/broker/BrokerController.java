package com.clouditora.mq.broker;

import com.clouditora.mq.broker.listener.BrokerChannelListener;
import com.clouditora.mq.broker.nameserver.NameserverRpcFacade;
import com.clouditora.mq.broker.processor.DefaultRequestProcessor;
import com.clouditora.mq.common.service.AbstractNothingService;
import com.clouditora.mq.common.util.ThreadUtil;
import com.clouditora.mq.network.Client;
import com.clouditora.mq.network.ClientNetworkConfig;
import com.clouditora.mq.network.Server;
import com.clouditora.mq.network.ServerNetworkConfig;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.*;

@Slf4j
public class BrokerController extends AbstractNothingService {
    private final BrokerConfig brokerConfig;
    private final ServerNetworkConfig serverNetworkConfig;
    private final ClientNetworkConfig clientNetworkConfig;
    private final Server server;
    private final Client client;
    private final ExecutorService nameserverRpcExecutor;
    private final NameserverRpcFacade nameserverRpcFacade;
    private final ScheduledExecutorService scheduledExecutor;

    public BrokerController(BrokerConfig brokerConfig, ServerNetworkConfig serverNetworkConfig, ClientNetworkConfig clientNetworkConfig) {
        this.brokerConfig = brokerConfig;
        this.serverNetworkConfig = serverNetworkConfig;
        this.clientNetworkConfig = clientNetworkConfig;
        this.server = new Server(this.serverNetworkConfig, new BrokerChannelListener());
        DefaultRequestProcessor requestProcessor = new DefaultRequestProcessor(brokerController);
        this.server.setDefaultProcessor(requestProcessor, null);
        this.client = new Client(clientNetworkConfig, null);
        this.nameserverRpcExecutor = new ThreadPoolExecutor(
                4, 10,
                1, TimeUnit.MINUTES,
                new ArrayBlockingQueue<>(32),
                ThreadUtil.buildFactory("RpcExecutor", 10)
        );
        this.nameserverRpcFacade = new NameserverRpcFacade(client, nameserverRpcExecutor);
        this.scheduledExecutor = new ScheduledThreadPoolExecutor(1, r -> new Thread(r, "ScheduledExecutor"));
    }

    @Override
    public String getServiceName() {
        return "Broker";
    }

    @Override
    public void startup() {
        this.client.updateNameserverEndpoints(this.brokerConfig.getNameserverEndpoints());
        this.server.startup();
        this.client.startup();
        registerBroker();
        // @link org.apache.rocketmq.common.BrokerConfig#registerNameServerPeriod
        this.scheduledExecutor.scheduleWithFixedDelay(this::registerBroker, 10, 30, TimeUnit.SECONDS);
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
        this.nameserverRpcFacade.registerBroker(
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
        this.nameserverRpcFacade.unregisterBroker(
                this.brokerConfig.getBrokerClusterName(),
                endpoint,
                endpoint,
                this.brokerConfig.getBrokerId(),
                this.brokerConfig.getRegisterBrokerTimeoutMills()
        );
    }
}
