package com.clouditora.mq.client.consumer;

import com.clouditora.mq.client.instance.ClientConfig;
import com.clouditora.mq.client.instance.ClientInstance;
import com.clouditora.mq.common.constant.MessageModel;
import com.clouditora.mq.common.constant.SystemTopic;
import com.clouditora.mq.common.service.AbstractNothingService;
import com.clouditora.mq.common.topic.ConsumerSubscription;
import com.clouditora.mq.common.topic.ConsumerSubscriptions;
import lombok.Getter;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Getter
public class Consumer extends AbstractNothingService {
    /**
     * @link org.apache.rocketmq.client.consumer.DefaultMQPullConsumer#consumerGroup
     */
    private final String group;
    /**
     * @link org.apache.rocketmq.client.consumer.DefaultMQPullConsumer#messageModel
     */
    private MessageModel messageModel = MessageModel.CLUSTERING;
    private final ConcurrentMap<String, ConsumerSubscriptions> subscriptionMap = new ConcurrentHashMap<>();

    public Consumer(String group) {
        this.group = group;
    }

    @Override
    public String getServiceName() {
        return "Consumer#" + group;
    }

    public Set<String> getTopics() {
        return this.subscriptionMap.keySet();
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
