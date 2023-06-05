package com.clouditora.mq.broker;

import com.clouditora.mq.broker.client.*;
import com.clouditora.mq.broker.dispatcher.*;
import com.clouditora.mq.broker.nameserver.NameserverApiFacade;
import com.clouditora.mq.common.constant.RpcModel;
import com.clouditora.mq.common.network.RequestCode;
import com.clouditora.mq.common.service.AbstractScheduledService;
import com.clouditora.mq.common.topic.GroupSubscription;
import com.clouditora.mq.common.topic.ProducerGroup;
import com.clouditora.mq.common.util.ThreadUtil;
import com.clouditora.mq.network.ClientNetwork;
import com.clouditora.mq.network.ClientNetworkConfig;
import com.clouditora.mq.network.ServerNetwork;
import com.clouditora.mq.network.ServerNetworkConfig;
import com.clouditora.mq.store.MessageStore;
import com.clouditora.mq.store.MessageStoreConfig;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

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
    private final ConsumerLockManager consumerLockManager;
    private final TopicQueueConfigManager topicQueueConfigManager;
    private final MessageStore messageStore;
    private final List<ExecutorService> executors = new ArrayList<>();

    public BrokerController(
            BrokerConfig brokerConfig,
            ServerNetworkConfig serverNetworkConfig,
            ClientNetworkConfig clientNetworkConfig,
            MessageStoreConfig messageStoreConfig
    ) {
        this.brokerConfig = brokerConfig;
        this.brokerConfig.setBrokerPort(serverNetworkConfig.getListenPort());
        this.nameserverApiExecutor = new ThreadPoolExecutor(
                4, 10,
                1, TimeUnit.MINUTES,
                new ArrayBlockingQueue<>(32),
                ThreadUtil.buildFactory("ApiExecutor", 10)
        );
        this.topicQueueConfigManager = new TopicQueueConfigManager(brokerConfig, messageStoreConfig, this);
        this.producerManager = new ProducerManager();
        this.consumerManager = new ConsumerManager(topicQueueConfigManager);
        this.consumerLockManager = new ConsumerLockManager();
        this.serverNetwork = new ServerNetwork(serverNetworkConfig, new ClientChannelListener(this));
        this.clientNetwork = new ClientNetwork(clientNetworkConfig, null);
        this.clientNetwork.updateNameserverEndpoints(this.brokerConfig.getNameserverEndpoints());
        this.nameserverApiFacade = new NameserverApiFacade(brokerConfig, clientNetwork, nameserverApiExecutor);

        this.messageStore = new MessageStore(messageStoreConfig);
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
        this.topicQueueConfigManager.startup();
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
        this.topicQueueConfigManager.shutdown();
        this.executors.forEach(ExecutorService::shutdown);
        super.shutdown();
    }

    /**
     * @link org.apache.rocketmq.broker.BrokerController#registerProcessor
     */
    private void registerDispatchers() {
        ExecutorService clientHeartbeatExecutor = new ThreadPoolExecutor(
                BrokerConfig.TreadPoolSize.CLIENT_HEARTBEAT,
                BrokerConfig.TreadPoolSize.CLIENT_HEARTBEAT,
                1, TimeUnit.MINUTES,
                new ArrayBlockingQueue<>(BrokerConfig.QueueCapacity.CLIENT_HEARTBEAT),
                ThreadUtil.buildFactory("ClientHeartbeat", BrokerConfig.TreadPoolSize.CLIENT_HEARTBEAT)
        );
        executors.add(clientHeartbeatExecutor);
        ExecutorService clientManageExecutor = new ThreadPoolExecutor(
                BrokerConfig.TreadPoolSize.CLIENT_MANAGER,
                BrokerConfig.TreadPoolSize.CLIENT_MANAGER,
                1, TimeUnit.MINUTES,
                new ArrayBlockingQueue<>(BrokerConfig.QueueCapacity.CLIENT_MANAGE),
                ThreadUtil.buildFactory("ClientManager", BrokerConfig.TreadPoolSize.CLIENT_MANAGER)
        );
        executors.add(clientManageExecutor);
        ExecutorService sendMessageExecutor = new ThreadPoolExecutor(
                BrokerConfig.TreadPoolSize.SEND_MESSAGE,
                BrokerConfig.TreadPoolSize.SEND_MESSAGE,
                1, TimeUnit.MINUTES,
                new ArrayBlockingQueue<>(BrokerConfig.QueueCapacity.SEND_MESSAGE),
                ThreadUtil.buildFactory("SendMessage", BrokerConfig.TreadPoolSize.SEND_MESSAGE)
        );
        executors.add(sendMessageExecutor);
        ExecutorService pullMessageExecutor = new ThreadPoolExecutor(
                BrokerConfig.TreadPoolSize.PULL_MESSAGE,
                BrokerConfig.TreadPoolSize.PULL_MESSAGE,
                1, TimeUnit.MINUTES,
                new ArrayBlockingQueue<>(BrokerConfig.QueueCapacity.PULL_MESSAGE),
                ThreadUtil.buildFactory("PullMessage", BrokerConfig.TreadPoolSize.PULL_MESSAGE)
        );
        executors.add(pullMessageExecutor);
        ExecutorService adminBrokerExecutor = new ThreadPoolExecutor(
                BrokerConfig.TreadPoolSize.ADMIN_BROKER,
                BrokerConfig.TreadPoolSize.ADMIN_BROKER,
                1, TimeUnit.MINUTES,
                new LinkedBlockingQueue<>(),
                ThreadUtil.buildFactory("AdminBroker", BrokerConfig.TreadPoolSize.ADMIN_BROKER)
        );
        executors.add(adminBrokerExecutor);
        ExecutorService consumerManageExecutor = new ThreadPoolExecutor(
                BrokerConfig.TreadPoolSize.CONSUMER_MANAGE,
                BrokerConfig.TreadPoolSize.CONSUMER_MANAGE,
                1, TimeUnit.MINUTES,
                new LinkedBlockingQueue<>(),
                ThreadUtil.buildFactory("ConsumerMange", BrokerConfig.TreadPoolSize.CONSUMER_MANAGE)
        );
        executors.add(consumerManageExecutor);

        ClientManageDispatcher clientManageDispatcher = new ClientManageDispatcher(this);
        this.serverNetwork.registerDispatcher(RequestCode.REGISTER_CLIENT, clientManageDispatcher, clientHeartbeatExecutor);
        this.serverNetwork.registerDispatcher(RequestCode.UNREGISTER_CLIENT, clientManageDispatcher, clientManageExecutor);

        ConsumerManageDispatcher consumerManageDispatcher = new ConsumerManageDispatcher(consumerManager);
        this.serverNetwork.registerDispatcher(RequestCode.GET_CONSUMER_LIST_BY_GROUP, consumerManageDispatcher, sendMessageExecutor);

        SendMessageDispatcher sendMessageDispatcher = new SendMessageDispatcher(this.brokerConfig, this.messageStore);
        this.serverNetwork.registerDispatcher(RequestCode.SEND_MESSAGE, sendMessageDispatcher, sendMessageExecutor);
        this.serverNetwork.registerDispatcher(RequestCode.SEND_MESSAGE_V2, sendMessageDispatcher, sendMessageExecutor);

        PullMessageDispatcher pullMessageDispatcher = new PullMessageDispatcher(this.brokerConfig);
        this.serverNetwork.registerDispatcher(RequestCode.PULL_MESSAGE, pullMessageDispatcher, pullMessageExecutor);

        AdminBrokerDispatcher adminBrokerDispatcher = new AdminBrokerDispatcher(this.consumerLockManager);
        this.serverNetwork.setDefaultDispatcher(adminBrokerDispatcher, adminBrokerExecutor);
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
                this.topicQueueConfigManager.getTopicMap()
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
    public void registerClient(ClientChannel channel, Set<ProducerGroup> producers, Set<GroupSubscription> consumers) {
        this.producerManager.register(channel, producers);
        this.consumerManager.register(channel, consumers);
    }

    /**
     * @link org.apache.rocketmq.broker.processor.ClientManageProcessor#unregisterClient
     */
    public void unregisterClient(ClientChannel channel, String producer, String consumer) {
        this.producerManager.unregister(channel, producer);
        this.consumerManager.unregister(channel, consumer);
    }

    public void unregisterClient(Channel channel) {
        this.producerManager.unregister(channel);
        this.consumerManager.unregister(channel);
    }

    /**
     * @link org.apache.rocketmq.broker.client.ClientHousekeepingService#scanExceptionChannel
     */
    private void cleanExpiredClient() {
        this.producerManager.cleanExpiredClient();
        this.consumerManager.cleanExpiredClient();
    }
}
