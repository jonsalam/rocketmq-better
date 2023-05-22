package com.clouditora.mq.broker;

import com.clouditora.mq.broker.client.*;
import com.clouditora.mq.broker.nameserver.NameserverApiFacade;
import com.clouditora.mq.common.constant.RpcModel;
import com.clouditora.mq.common.network.RequestCode;
import com.clouditora.mq.common.service.AbstractScheduledService;
import com.clouditora.mq.common.topic.ConsumerSubscriptions;
import com.clouditora.mq.common.topic.ProducerGroup;
import com.clouditora.mq.common.util.ThreadUtil;
import com.clouditora.mq.network.ClientNetwork;
import com.clouditora.mq.network.ClientNetworkConfig;
import com.clouditora.mq.network.ServerNetwork;
import com.clouditora.mq.network.ServerNetworkConfig;
import lombok.extern.slf4j.Slf4j;

import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @link org.apache.rocketmq.broker.BrokerController
 */
@Slf4j
public class BrokerController extends AbstractScheduledService {
    private final BrokerConfig brokerConfig;
    private final ServerNetwork serverNetwork;
    private final ClientNetwork clientNetwork;
    private final ExecutorService nameserverApiExecutor;
    private final NameserverApiFacade nameserverApiFacade;
    private final ProducerManager producerManager;
    private final ConsumerManager consumerManager;
    private final TopicQueueManager topicQueueManager;

    public BrokerController(BrokerConfig brokerConfig, ServerNetworkConfig serverNetworkConfig, ClientNetworkConfig clientNetworkConfig) {
        this.brokerConfig = brokerConfig;
        this.brokerConfig.setBrokerPort(serverNetworkConfig.getListenPort());
        this.nameserverApiExecutor = new ThreadPoolExecutor(
                4, 10,
                1, TimeUnit.MINUTES,
                new ArrayBlockingQueue<>(32),
                ThreadUtil.buildFactory("ApiExecutor", 10)
        );
        this.topicQueueManager = new TopicQueueManager(brokerConfig, this);
        this.producerManager = new ProducerManager();
        this.consumerManager = new ConsumerManager(topicQueueManager);
        this.serverNetwork = new ServerNetwork(serverNetworkConfig, new ClientChannelListener(producerManager, consumerManager));
        this.clientNetwork = new ClientNetwork(clientNetworkConfig, null);
        this.clientNetwork.updateNameserverEndpoints(this.brokerConfig.getNameserverEndpoints());
        this.nameserverApiFacade = new NameserverApiFacade(brokerConfig, clientNetwork, nameserverApiExecutor);
    }

    @Override
    public String getServiceName() {
        return "Broker";
    }

    @Override
    public void startup() {
        registerDispatchers();
        this.serverNetwork.startup();
        this.clientNetwork.startup();
        this.topicQueueManager.startup();
        registerBroker();
        scheduled(10_000, brokerConfig.getRegisterNameServerPeriod(), this::registerBroker);
        scheduled(10_000, 10_000, this::cleanExpiredClient);
        super.startup();
    }

    @Override
    public void shutdown() {
        unregisterBroker();
        this.serverNetwork.shutdown();
        this.clientNetwork.shutdown();
        this.nameserverApiExecutor.shutdown();
        this.topicQueueManager.shutdown();
        super.shutdown();
    }

    private void registerDispatchers() {
        ExecutorService clientHeartbeatExecutor = new ThreadPoolExecutor(
                BrokerConfig.TreadPoolSize.heartbeat,
                BrokerConfig.TreadPoolSize.heartbeat,
                60, TimeUnit.MINUTES,
                new ArrayBlockingQueue<>(32),
                ThreadUtil.buildFactory("Heartbeat", BrokerConfig.TreadPoolSize.heartbeat)
        );
        ExecutorService clientManageExecutor = new ThreadPoolExecutor(
                BrokerConfig.TreadPoolSize.clientManager,
                BrokerConfig.TreadPoolSize.clientManager,
                60, TimeUnit.MINUTES,
                new ArrayBlockingQueue<>(32),
                ThreadUtil.buildFactory("Client", BrokerConfig.TreadPoolSize.clientManager)
        );

        ClientCommandDispatcher clientDispatcher = new ClientCommandDispatcher(this);
        this.serverNetwork.registerDispatcher(RequestCode.REGISTER_CLIENT, clientDispatcher, clientHeartbeatExecutor);
        this.serverNetwork.registerDispatcher(RequestCode.UNREGISTER_CLIENT, clientDispatcher, clientManageExecutor);
    }

    public void registerBroker() {
        registerBroker(RpcModel.SYNC);
    }

    /**
     * @link org.apache.rocketmq.broker.BrokerController#registerBrokerAll
     */
    public void registerBroker(RpcModel rpcModel) {
        this.nameserverApiFacade.registerBroker(
                rpcModel,
                this.brokerConfig.getBrokerClusterName(),
                this.brokerConfig.getBrokerName(),
                this.brokerConfig.getBrokerEndpoint(),
                this.brokerConfig.getBrokerId(),
                this.topicQueueManager.getTopicMap()
        );
    }

    /**
     * @link org.apache.rocketmq.broker.BrokerController#unregisterBrokerAll
     */
    private void unregisterBroker() {
        this.nameserverApiFacade.unregisterBroker(
                this.brokerConfig.getBrokerClusterName(),
                this.brokerConfig.getBrokerName(),
                this.brokerConfig.getBrokerEndpoint(),
                this.brokerConfig.getBrokerId()
        );
    }

    /**
     * @link org.apache.rocketmq.broker.processor.ClientManageProcessor#heartBeat
     */
    public void registerClient(ClientChannel channel, Set<ProducerGroup> producers, Set<ConsumerSubscriptions> consumers) {
        producerManager.register(channel, producers);
        consumerManager.register(channel, consumers);
    }

    /**
     * @link org.apache.rocketmq.broker.processor.ClientManageProcessor#unregisterClient
     */
    public void unregisterClient(ClientChannel channel, String producer, String consumer) {
        producerManager.unregister(channel, producer);
        consumerManager.unregister(channel, consumer);
    }

    /**
     * @link org.apache.rocketmq.broker.client.ClientHousekeepingService#scanExceptionChannel
     */
    private void cleanExpiredClient() {
        producerManager.cleanExpiredClient();
        consumerManager.cleanExpiredClient();
    }
}
