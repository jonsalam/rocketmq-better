package com.clouditora.mq.client.instance;

import com.clouditora.mq.client.broker.BrokerApiFacade;
import com.clouditora.mq.client.broker.BrokerManager;
import com.clouditora.mq.client.consumer.Consumer;
import com.clouditora.mq.client.consumer.ConsumerManager;
import com.clouditora.mq.client.nameserver.NameserverApiFacade;
import com.clouditora.mq.client.producer.Producer;
import com.clouditora.mq.client.producer.ProducerManager;
import com.clouditora.mq.client.topic.MessageRoute;
import com.clouditora.mq.client.topic.MessageRouteManager;
import com.clouditora.mq.client.topic.TopicRouteManager;
import com.clouditora.mq.common.Message;
import com.clouditora.mq.common.concurrent.ConsumeStrategy;
import com.clouditora.mq.common.constant.ConsumePositionStrategy;
import com.clouditora.mq.common.constant.GlobalConstant;
import com.clouditora.mq.common.constant.RpcModel;
import com.clouditora.mq.common.exception.BrokerException;
import com.clouditora.mq.common.message.MessageQueue;
import com.clouditora.mq.common.message.SendResult;
import com.clouditora.mq.common.service.AbstractScheduledService;
import com.clouditora.mq.common.topic.ConsumerSubscriptions;
import com.clouditora.mq.common.topic.ProducerGroup;
import com.clouditora.mq.common.topic.TopicRoute;
import com.clouditora.mq.common.util.NetworkUtil;
import com.clouditora.mq.network.ClientNetwork;
import com.clouditora.mq.network.ClientNetworkConfig;
import com.clouditora.mq.network.exception.ConnectException;
import com.clouditora.mq.network.exception.TimeoutException;
import lombok.extern.slf4j.Slf4j;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @link org.apache.rocketmq.client.impl.factory.MQClientInstance
 */
@Slf4j
public class ClientInstance extends AbstractScheduledService {
    /**
     * @link org.apache.rocketmq.client.impl.factory.MQClientInstance#LOCK_TIMEOUT_MILLIS
     */
    private final static long LOCK_TIMEOUT_MILLIS = 3000;

    private final String clientId;
    private final ClientConfig clientConfig;
    private final ClientNetwork clientNetwork;
    private final NameserverApiFacade nameserverApiFacade;
    private final BrokerManager brokerManager;
    private final ProducerManager producerManager;
    private final ConsumerManager consumerManager;
    private final TopicRouteManager topicRouteManager;
    private final MessageRouteManager messageRouteManager;
    private final Lock lock = new ReentrantLock();

    /**
     * @link org.apache.rocketmq.client.impl.factory.MQClientInstance#MQClientInstance
     */
    public ClientInstance(ClientConfig clientConfig) {
        String instanceName = clientConfig.getInstanceName();
        if (ClientConfig.DEFAULT_INSTANCE.equals(instanceName)) {
            // @link org.apache.rocketmq.client.ClientConfig#changeInstanceNameToPID
            clientConfig.setInstanceName(String.format("%s@%s#%d", GlobalConstant.PID, NetworkUtil.getLocalIp(), System.nanoTime()));
        }

        this.clientId = clientConfig.buildClientId();
        this.clientConfig = clientConfig;
        ClientNetworkConfig networkConfig = new ClientNetworkConfig();
        networkConfig.setClientCallbackExecutorThreads(clientConfig.getClientCallbackExecutorThreads());
        this.clientNetwork = new ClientNetwork(networkConfig, null);
        this.clientNetwork.updateNameserverEndpoints(clientConfig.getNameserverEndpoints());
        registerDispatchers();
        this.nameserverApiFacade = new NameserverApiFacade(clientConfig, this.clientNetwork);
        BrokerApiFacade brokerApiFacade = new BrokerApiFacade(clientConfig, this.clientNetwork);
        this.brokerManager = new BrokerManager(brokerApiFacade);
        this.producerManager = new ProducerManager();
        this.consumerManager = new ConsumerManager();
        this.topicRouteManager = new TopicRouteManager();
        this.messageRouteManager = new MessageRouteManager();
        registerDefaultProducer();
    }

    @Override
    public String getServiceName() {
        return this.clientConfig.getInstanceName();
    }

    @Override
    public void startup() {
        this.clientNetwork.startup();
        scheduled(TimeUnit.MILLISECONDS, 10, this.clientConfig.getPollNameServerInterval(), this::updateTopicRoute);
        scheduled(TimeUnit.MILLISECONDS, 1000, this.clientConfig.getHeartbeatBrokerInterval(), this::heartbeat);
        super.startup();
    }

    @Override
    public void shutdown() {
        unregisterProducers();
        unregisterConsumers();
        this.clientNetwork.shutdown();
        super.shutdown();
    }

    /**
     * @link org.apache.rocketmq.client.impl.MQClientAPIImpl#MQClientAPIImpl
     */
    private void registerDispatchers() {
//        this.client.registerDispatcher(RequestCode.GET_CONSUMER_RUNNING_INFO, this.clientDispatcher, null);
    }

    /**
     * like org.apache.rocketmq.common.MixAll#CLIENT_INNER_PRODUCER_GROUP = CLIENT_INNER_PRODUCER
     */
    private void registerDefaultProducer() {
        producerManager.register(new Producer(GlobalConstant.SystemGroup.CLIENT_INNER_PRODUCER));
    }

    /**
     * @link org.apache.rocketmq.client.impl.factory.MQClientInstance#registerProducer
     */
    public void registerProducer(Producer producer) {
        this.producerManager.register(producer);
        producer.setClientInstance(this);
    }

    /**
     * @link org.apache.rocketmq.client.impl.factory.MQClientInstance#unregisterProducer
     */
    public void unregisterProducer(String group) {
        this.producerManager.unregister(group);
        this.brokerManager.unregisterClient(this.clientId, group, null);
    }

    private void unregisterProducers() {
        this.producerManager.getGroups().forEach(this::unregisterProducer);
    }

    /**
     * @link org.apache.rocketmq.client.impl.factory.MQClientInstance#registerConsumer
     */
    public void registerConsumer(Consumer consumer) {
        this.consumerManager.register(consumer);
    }

    /**
     * @link org.apache.rocketmq.client.impl.factory.MQClientInstance#unregisterConsumer
     */
    public void unregisterConsumer(String group) {
        this.consumerManager.unregister(group);
        this.brokerManager.unregisterClient(this.clientId, null, group);
    }

    private void unregisterConsumers() {
        this.consumerManager.getGroups().forEach(this::unregisterConsumer);
    }

    /**
     * @link org.apache.rocketmq.client.impl.factory.MQClientInstance#updateTopicRouteInfoFromNameServer
     */
    protected void updateTopicRoute() {
        List<String> consumerTopics = this.consumerManager.getTopics();
        List<String> producerTopics = this.producerManager.getTopics();
        Set<String> topics = Stream.of(consumerTopics, producerTopics)
                .filter(Objects::nonNull)
                .flatMap(Collection::stream)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
        for (String topic : topics) {
            updateTopicRoute(topic);
        }
    }

    /**
     * @link org.apache.rocketmq.client.impl.factory.MQClientInstance#updateTopicRouteInfoFromNameServer
     */
    public void updateTopicRoute(String topic) {
        try {
            if (!this.lock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                log.error("update topic route {} wait timeout", topic);
                return;
            }
            try {
                TopicRoute topicRoute = this.nameserverApiFacade.getTopicRoute(topic);
                TopicRoute prevTopicRoute = this.topicRouteManager.get(topic);
                // TODO 优化
                if (!topicRoute.equals(prevTopicRoute)) {
                    log.debug("update topic route {}: {}", topic, topicRoute);
                    this.brokerManager.addBrokers(topicRoute.getBrokers());
                    if (this.producerManager.isNotEmpty()) {
                        MessageRoute messageRoute = MessageRoute.build(topic, topicRoute);
                        this.messageRouteManager.put(topic, messageRoute);
                    }
                    this.topicRouteManager.put(topic, topicRoute);
                }
            } catch (Exception e) {
                log.error("update topic route {} exception", topic, e);
            } finally {
                this.lock.unlock();
            }
        } catch (InterruptedException e) {
            log.error("update topic route {} exception", topic, e);
        }
    }

    /**
     * @link org.apache.rocketmq.client.impl.factory.MQClientInstance#sendHeartbeatToAllBroker
     */
    protected void heartbeat() {
        Set<ProducerGroup> producers = this.producerManager.getGroups().stream().map(e -> {
                    ProducerGroup producer = new ProducerGroup();
                    producer.setGroup(e);
                    return producer;
                })
                .collect(Collectors.toSet());
        Set<ConsumerSubscriptions> consumers = this.consumerManager.getConsumerMap().entrySet().stream()
                .map(e -> {
                    String group = e.getKey();
                    Consumer consumer = e.getValue();

                    ConsumerSubscriptions subscriptions = new ConsumerSubscriptions();
                    subscriptions.setGroup(group);
                    subscriptions.setConsumeStrategy(ConsumeStrategy.PUSH);
                    subscriptions.setMessageModel(consumer.getMessageModel());
                    subscriptions.setPositionStrategy(ConsumePositionStrategy.FROM_FIRST_OFFSET);
                    subscriptions.setSubscriptions(consumer.getSubscriptions());
                    return subscriptions;
                })
                .collect(Collectors.toSet());

        this.brokerManager.heartbeat(this.clientId, producers, consumers);
    }

    public MessageRoute getMessageRoute(String topic) {
        return this.messageRouteManager.get(topic);
    }

    public SendResult send(RpcModel rpcModel, String group, MessageQueue queue, Message message, long timeout) throws InterruptedException, TimeoutException, ConnectException, BrokerException {
        return this.brokerManager.send(rpcModel, group, queue, message, timeout);
    }
}
