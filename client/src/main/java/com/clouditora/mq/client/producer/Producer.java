package com.clouditora.mq.client.producer;

import com.clouditora.mq.client.instance.ClientConfig;
import com.clouditora.mq.client.instance.ClientInstance;
import com.clouditora.mq.client.topic.MessageRoute;
import com.clouditora.mq.common.Message;
import com.clouditora.mq.common.constant.ClientErrorCode;
import com.clouditora.mq.common.constant.RpcModel;
import com.clouditora.mq.common.constant.SendStatus;
import com.clouditora.mq.common.exception.BrokerException;
import com.clouditora.mq.common.exception.ClientException;
import com.clouditora.mq.common.message.MessageQueue;
import com.clouditora.mq.common.message.SendResult;
import com.clouditora.mq.network.exception.ConnectException;
import com.clouditora.mq.network.exception.TimeoutException;
import com.clouditora.mq.network.protocol.ResponseCode;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * @link org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl
 */
@Slf4j
@Getter
public class Producer {
    @Setter
    private ClientInstance clientInstance;
    private String group;
    protected final ProducerConfig producerConfig;

    public Producer(String group) {
        super();
        this.group = group;
        this.producerConfig = new ProducerConfig();
        this.producerConfig.setProducerGroup(group);
    }

    private void send(Message message) throws InterruptedException, BrokerException, ClientException {
        send(RpcModel.SYNC, message);
    }

    /**
     * @link org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl#sendDefaultImpl
     */
    public SendResult send(RpcModel rpcModel, Message message) throws InterruptedException, BrokerException, ClientException {
        MessageRoute route = this.clientInstance.getMessageRoute(message.getTopic());
        if (route == null || route.isEmpty()) {
            this.clientInstance.updateTopicRoute(message.getTopic());
        }
        route = this.clientInstance.getMessageRoute(message.getTopic());
        if (route == null || route.isEmpty()) {
            throw new ClientException(ClientErrorCode.NOT_FOUND_TOPIC_EXCEPTION, "no route of this topic: %s".formatted(message.getTopic()));
        }

        try {
            if (rpcModel == RpcModel.SYNC) {
                return syncSend(route, message);
            } else {
                asyncSend(rpcModel, route, message);
                // 异步没有返回值
                return null;
            }
        } catch (ConnectException e) {
            throw new ClientException(ClientErrorCode.CONNECT_BROKER_EXCEPTION, "send message failed", e);
        } catch (TimeoutException e) {
            throw new ClientException(ClientErrorCode.ACCESS_BROKER_TIMEOUT, "send message failed", e);
        }
    }

    private SendResult syncSend(MessageRoute route, Message message) throws InterruptedException, BrokerException, ConnectException, TimeoutException {
        long startTime = System.currentTimeMillis();
        SendResult result = null;
        MessageQueue messageQueue = null;
        int count = producerConfig.isRetryAnotherBrokerWhenNotStoreOK() ? producerConfig.getRetryTimesWhenSendFailed() + 1 : 1;
        for (int i = 0; i < count; i++) {
            String prevBrokerName = Optional.ofNullable(messageQueue).map(MessageQueue::getBrokerName).orElse(null);
            messageQueue = route.findOne(prevBrokerName);
            long elapsed = System.currentTimeMillis() - startTime;
            long timeout = producerConfig.getSendMsgTimeout() - elapsed;
            if (timeout < 0) {
                throw new TimeoutException("timeout");
            }
            try {
                result = this.clientInstance.send(RpcModel.SYNC, producerConfig.getProducerGroup(), messageQueue, message, timeout);
                if (result.getSendStatus() == SendStatus.SEND_OK) {
                    return result;
                }
            } catch (BrokerException e) {
                if (ResponseCode.RETRY_RESPONSE_CODES.contains(e.getCode())) {
                    log.debug("broker exception: {}", e.getCode());
                    continue;
                }
                throw e;
            }
        }
        return result;
    }

    private void asyncSend(RpcModel rpcModel, MessageRoute route, Message message) throws InterruptedException, BrokerException, ConnectException, TimeoutException {
        MessageQueue messageQueue = route.findOne();
        this.clientInstance.send(rpcModel, producerConfig.getProducerGroup(), messageQueue, message, producerConfig.getSendMsgTimeout());
    }

    public static void main(String[] args) throws InterruptedException {
        ClientConfig config = new ClientConfig();
        Producer producer = new Producer("test");
        ClientInstance instance = new ClientInstance(config);
        instance.registerProducer(producer);
        instance.startup();
        Message message = new Message("SELF_TEST_TOPIC", "Tag1", "hello world".getBytes(StandardCharsets.UTF_8));
        try {
            producer.send(message);
        } catch (Exception e) {
            e.printStackTrace();
        }
        TimeUnit.SECONDS.sleep(1);
        instance.shutdown();
    }
}
