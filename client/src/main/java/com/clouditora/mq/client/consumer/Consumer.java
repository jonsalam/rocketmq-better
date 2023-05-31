package com.clouditora.mq.client.consumer;

import com.clouditora.mq.client.consumer.consume.ConcurrentMessageConsumeService;
import com.clouditora.mq.client.consumer.consume.MessageConsumeService;
import com.clouditora.mq.client.consumer.consume.OrderMessageConsumeService;
import com.clouditora.mq.client.consumer.listener.ConcurrentMessageListener;
import com.clouditora.mq.client.consumer.listener.MessageListener;
import com.clouditora.mq.client.consumer.listener.OrderMessageListener;
import com.clouditora.mq.client.consumer.offset.AbstractOffsetManager;
import com.clouditora.mq.client.consumer.offset.LocalOffsetManager;
import com.clouditora.mq.client.consumer.offset.RemoteOffsetManager;
import com.clouditora.mq.client.consumer.pull.MessagePullService;
import com.clouditora.mq.client.instance.ClientConfig;
import com.clouditora.mq.client.instance.ClientInstance;
import com.clouditora.mq.common.constant.MessageModel;
import com.clouditora.mq.common.constant.SystemTopic;
import com.clouditora.mq.common.service.AbstractNothingService;
import com.clouditora.mq.common.topic.ConsumerSubscription;
import com.clouditora.mq.common.topic.ConsumerSubscriptions;
import lombok.Getter;
import lombok.Setter;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class Consumer extends AbstractNothingService {
    @Setter
    private ClientInstance clientInstance;
    /**
     * @link org.apache.rocketmq.client.consumer.DefaultMQPullConsumer#consumerGroup
     */
    private final String group;
    /**
     * @link org.apache.rocketmq.client.consumer.DefaultMQPullConsumer#messageModel
     */
    @Getter
    private MessageModel messageModel = MessageModel.CLUSTERING;
    /**
     * topic:
     */
    private final ConcurrentMap<String, ConsumerSubscriptions> subscriptionMap = new ConcurrentHashMap<>();

    private AbstractOffsetManager offsetManager;
    private MessagePullService messagePullService;
    private MessageConsumeService messageConsumeService;
    private MessageListener messageListener;

    public Consumer(String group) {
        this.group = group;
    }

    @Override
    public String getServiceName() {
        return "Consumer#" + group;
    }

    @Override
    public void startup() {
        if (this.messageModel == MessageModel.BROADCASTING) {
            this.offsetManager = new LocalOffsetManager(this.clientInstance.getClientId(), this.group);
        } else {
            this.offsetManager = new RemoteOffsetManager(this.group, this.clientInstance);
        }
        this.offsetManager.startup();

        this.messagePullService = new MessagePullService();
        this.messagePullService.startup();

        if (messageListener instanceof OrderMessageListener) {
            this.messageConsumeService = new OrderMessageConsumeService();
        } else {
            this.messageConsumeService = new ConcurrentMessageConsumeService();
        }
        this.messageConsumeService.startup();

        super.startup();
    }

    @Override
    public void shutdown() {
        this.offsetManager.shutdown();
        this.messagePullService.shutdown();
        this.messageConsumeService.shutdown();
        super.shutdown();
    }

    public Set<String> getTopics() {
        return this.subscriptionMap.keySet();
    }

    public ConsumerSubscriptions getSubscriptions(String topic) {
        return this.subscriptionMap.get(topic);
    }

    public void subscribe(String topic, String expression) {
        ConsumerSubscriptions subscriptions = this.subscriptionMap.computeIfAbsent(topic, e -> new ConsumerSubscriptions());
        ConsumerSubscription subscription = new ConsumerSubscription();
        subscription.setTopic(topic);
        subscription.setExpression(expression);
        subscriptions.add(subscription);
    }

    public Set<ConsumerSubscription> getSubscriptions() {
        return this.subscriptionMap.values().stream()
                .map(ConsumerSubscriptions::getSubscriptions)
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
    }

    /**
     * @link org.apache.rocketmq.client.consumer.DefaultMQPushConsumer#registerMessageListener
     */
    public void registerListener(ConcurrentMessageListener messageListener) {
        this.messageListener = messageListener;
    }

    /**
     * @link org.apache.rocketmq.client.consumer.DefaultMQPushConsumer#registerMessageListener
     */
    public void registerListener(OrderMessageListener messageListener) {
        this.messageListener = messageListener;
    }

    public static void main(String[] args) throws Exception {
        ClientConfig config = new ClientConfig();
        ClientInstance instance = new ClientInstance(config);
        Consumer consumer = new Consumer("test");
        consumer.subscribe(SystemTopic.SELF_TEST_TOPIC.getTopic(), "*");
        instance.registerConsumer(consumer);
        instance.startup();
        TimeUnit.SECONDS.sleep(10);
        instance.shutdown();
    }
}
