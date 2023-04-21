package com.clouditora.mq.broker;

import com.clouditora.mq.broker.client.TopicManager;
import com.clouditora.mq.broker.nameserver.NameserverApiFacade;
import com.clouditora.mq.common.constant.RpcModel;
import com.clouditora.mq.common.service.AbstractScheduledService;
import com.clouditora.mq.common.util.ThreadUtil;
import com.clouditora.mq.network.ClientNetwork;
import com.clouditora.mq.network.ClientNetworkConfig;
import com.clouditora.mq.network.ServerNetwork;
import com.clouditora.mq.network.ServerNetworkConfig;
import lombok.extern.slf4j.Slf4j;

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
    private final TopicManager topicManager;

    public BrokerController(BrokerConfig brokerConfig, ServerNetworkConfig serverNetworkConfig, ClientNetworkConfig clientNetworkConfig) {
        this.brokerConfig = brokerConfig;
        this.brokerConfig.setBrokerPort(serverNetworkConfig.getListenPort());
        this.nameserverApiExecutor = new ThreadPoolExecutor(
                4, 10,
                1, TimeUnit.MINUTES,
                new ArrayBlockingQueue<>(32),
                ThreadUtil.buildFactory("ApiExecutor", 10)
        );
        this.topicManager = new TopicManager(brokerConfig);
        this.serverNetwork = new ServerNetwork(serverNetworkConfig, null);
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
        this.serverNetwork.startup();
        this.clientNetwork.startup();
        this.topicManager.startup();
        registerBroker();
        scheduled(10_000, brokerConfig.getRegisterNameServerPeriod(), this::registerBroker);
        super.startup();
    }

    @Override
    public void shutdown() {
        unregisterBroker();
        this.serverNetwork.shutdown();
        this.clientNetwork.shutdown();
        this.nameserverApiExecutor.shutdown();
        this.topicManager.shutdown();
        super.shutdown();
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
                this.topicManager.getTopicMap()
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
}
