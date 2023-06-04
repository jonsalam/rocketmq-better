package com.clouditora.mq.client.consumer;

import com.clouditora.mq.client.consumer.consume.AbstractMessageConsumer;
import com.clouditora.mq.client.consumer.consume.ConcurrentMessageConsumer;
import com.clouditora.mq.client.consumer.consume.OrderMessageConsumer;
import com.clouditora.mq.client.consumer.listener.ConcurrentMessageListener;
import com.clouditora.mq.client.consumer.listener.MessageListener;
import com.clouditora.mq.client.consumer.listener.OrderMessageListener;
import com.clouditora.mq.client.consumer.offset.AbstractOffsetManager;
import com.clouditora.mq.client.consumer.offset.LocalOffsetManager;
import com.clouditora.mq.client.consumer.offset.RemoteOffsetManager;
import com.clouditora.mq.client.consumer.pull.PullMessageService;
import com.clouditora.mq.client.instance.ClientConfig;
import com.clouditora.mq.client.instance.ClientInstance;
import com.clouditora.mq.common.constant.MessageModel;
import com.clouditora.mq.common.constant.SystemTopic;
import com.clouditora.mq.common.service.AbstractNothingService;
import com.clouditora.mq.common.topic.TopicSubscription;
import com.clouditora.mq.common.topic.GroupSubscription;
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
    @Getter
    private boolean orderly;
    /**
     * topic:
     */
    private final ConcurrentMap<String, GroupSubscription> subscriptionsMap = new ConcurrentHashMap<>();

    private AbstractOffsetManager offsetManager;
    private PullMessageService pullMessageService;
    private AbstractMessageConsumer messageConsumer;
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

        this.pullMessageService = new PullMessageService();
        this.pullMessageService.startup();

        if (messageListener instanceof OrderMessageListener) {
            this.messageConsumer = new OrderMessageConsumer();
        } else {
            this.messageConsumer = new ConcurrentMessageConsumer();
        }
        this.messageConsumer.startup();

        super.startup();
    }

    @Override
    public void shutdown() {
        this.offsetManager.shutdown();
        this.pullMessageService.shutdown();
        this.messageConsumer.shutdown();
        super.shutdown();
    }

    public Set<String> getTopics() {
        return this.subscriptionsMap.keySet();
    }

    public GroupSubscription getSubscriptions(String topic) {
        return this.subscriptionsMap.get(topic);
    }

    public void subscribe(String topic, String expression) {
        GroupSubscription subscriptions = this.subscriptionsMap.computeIfAbsent(topic, e -> new GroupSubscription());
        TopicSubscription subscription = new TopicSubscription();
        subscription.setTopic(topic);
        subscription.setExpression(expression);
        subscriptions.add(subscription);
    }

    public Set<TopicSubscription> getSubscriptions() {
        return this.subscriptionsMap.values().stream()
                .map(GroupSubscription::getSubscriptions)
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
