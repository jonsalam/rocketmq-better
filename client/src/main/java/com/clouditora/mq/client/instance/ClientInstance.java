package com.clouditora.mq.client.instance;

import com.clouditora.mq.client.broker.BrokerApiFacade;
import com.clouditora.mq.client.broker.BrokerManager;
import com.clouditora.mq.client.consumer.Consumer;
import com.clouditora.mq.client.consumer.ConsumerManager;
import com.clouditora.mq.client.nameserver.NameserverApiFacade;
import com.clouditora.mq.client.producer.Producer;
import com.clouditora.mq.client.producer.ProducerManager;
import com.clouditora.mq.client.topic.TopicRouteManager;
import com.clouditora.mq.common.command.protocol.ClientHeartBeatCommand;
import com.clouditora.mq.common.command.protocol.TopicRouteCommand;
import com.clouditora.mq.common.constant.GlobalConstant;
import com.clouditora.mq.common.service.AbstractScheduledService;
import com.clouditora.mq.network.Client;
import com.clouditora.mq.network.ClientNetworkConfig;
import lombok.extern.slf4j.Slf4j;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @link org.apache.rocketmq.client.impl.factory.MQClientInstance
 */
@Slf4j
public class ClientInstance extends AbstractScheduledService {
    private final String clientId;
    private final ClientConfig clientConfig;
    private final Client client;
    private final NameserverApiFacade nameserverApiFacade;
    private final BrokerManager brokerManager;
    private final ProducerManager producerManager;
    private final ConsumerManager consumerManager;
    private final TopicRouteManager topicRouteManager;

    /**
     * @link org.apache.rocketmq.client.impl.factory.MQClientInstance#MQClientInstance(ClientConfig, int, String, RPCHook)
     */
    public ClientInstance(ClientConfig clientConfig) {
        String instanceName = clientConfig.getInstanceName();
        if (ClientConfig.DEFAULT_INSTANCE.equals(instanceName)) {
            // @link org.apache.rocketmq.client.ClientConfig#changeInstanceNameToPID
            clientConfig.setInstanceName(String.format("%s#%d", GlobalConstant.PID, System.nanoTime()));
        }

        this.clientId = clientConfig.buildClientId();
        this.clientConfig = clientConfig;
        ClientNetworkConfig networkConfig = new ClientNetworkConfig();
        networkConfig.setClientCallbackExecutorThreads(clientConfig.getClientCallbackExecutorThreads());
        this.client = new Client(networkConfig, null);
        this.client.updateNameserverEndpoints(this.clientConfig.getNameserverEndpoints());
        registerProcessors();
        this.nameserverApiFacade = new NameserverApiFacade(this.client, clientConfig);
        BrokerApiFacade brokerApiFacade = new BrokerApiFacade(this.client);
        this.brokerManager = new BrokerManager(brokerApiFacade);
        this.producerManager = new ProducerManager();
        this.consumerManager = new ConsumerManager();
        this.topicRouteManager = new TopicRouteManager();
        registerDefaultProducer();
    }

    @Override
    public String getServiceName() {
        return "Client@" + this.clientConfig.getInstanceName();
    }

    @Override
    public void startup() {
        this.client.startup();
        scheduled(TimeUnit.MILLISECONDS, 10, this.clientConfig.getPollNameServerInterval(), this::updateTopicRouteInfo);
        scheduled(TimeUnit.MILLISECONDS, 1000, this.clientConfig.getHeartbeatBrokerInterval(), this::heartbeat);
        super.startup();
    }

    /**
     * @link org.apache.rocketmq.client.impl.MQClientAPIImpl#MQClientAPIImpl
     */
    private void registerProcessors() {
//        this.client.registerProcessor(RequestCode.GET_CONSUMER_RUNNING_INFO, this.clientProcessor, null);
    }

    /**
     * like org.apache.rocketmq.common.MixAll#CLIENT_INNER_PRODUCER_GROUP = CLIENT_INNER_PRODUCER
     */
    private void registerDefaultProducer() {

    }

    /**
     * @link org.apache.rocketmq.client.impl.factory.MQClientInstance#registerProducer
     */
    public void registerProducer(Producer producer) {
        this.producerManager.register(producer);
    }

    /**
     * @link org.apache.rocketmq.client.impl.factory.MQClientInstance#registerConsumer
     */
    public void registerConsumer(String group, Consumer consumer) {
        this.consumerManager.register(group, consumer);
    }

    /**
     * @link org.apache.rocketmq.client.impl.factory.MQClientInstance#unregisterConsumer
     */
    public void unregisterConsumer(String group) {
        this.consumerManager.unregister(group);
        this.brokerManager.unregisterClient(group);
    }

    /**
     * @link org.apache.rocketmq.client.impl.factory.MQClientInstance#updateTopicRouteInfoFromNameServer
     */
    protected void updateTopicRouteInfo() {
        List<String> consumerTopics = this.consumerManager.getTopics();
        List<String> producerTopics = this.producerManager.getTopics();
        Set<String> topics = Stream.of(consumerTopics, producerTopics).flatMap(Collection::stream).collect(Collectors.toSet());
        for (String topic : topics) {
            updateTopicRouteInfo(topic);
        }
    }

    /**
     * @link org.apache.rocketmq.client.impl.factory.MQClientInstance#updateTopicRouteInfoFromNameServer
     */
    private void updateTopicRouteInfo(String topic) {
        try {
            TopicRouteCommand.ResponseBody route = this.nameserverApiFacade.getTopicRoute(topic);
            TopicRouteCommand.ResponseBody prev = this.topicRouteManager.getTopicRoute(topic);
            if (!route.equals(prev)) {
                this.brokerManager.addBrokers(route.getBrokers());
                this.topicRouteManager.addTopicRoute(topic, route);
            }
        } catch (Exception e) {

        }
    }

    /**
     * @link org.apache.rocketmq.client.impl.factory.MQClientInstance#sendHeartbeatToAllBroker
     */
    protected void heartbeat() {
        Set<String> producerGroups = this.producerManager.groups();
        ClientHeartBeatCommand.RequestBody heartBeat = new ClientHeartBeatCommand.RequestBody();
        heartBeat.setClientId(this.clientId);
        Set<ClientHeartBeatCommand.ProducerData> producers = producerGroups.stream().map(e -> new ClientHeartBeatCommand.ProducerData()).collect(Collectors.toSet());
        heartBeat.setProducers(producers);
        this.brokerManager.heartbeat(heartBeat);
    }
}
