package com.clouditora.mq.client.broker;

import com.clouditora.mq.client.consumer.pull.PullResult;
import com.clouditora.mq.client.consumer.pull.PullStatus;
import com.clouditora.mq.client.instance.ClientConfig;
import com.clouditora.mq.client.producer.SendResult;
import com.clouditora.mq.client.producer.SendStatus;
import com.clouditora.mq.common.Message;
import com.clouditora.mq.common.MessageConst;
import com.clouditora.mq.common.constant.RpcModel;
import com.clouditora.mq.common.exception.BrokerException;
import com.clouditora.mq.common.network.RequestCode;
import com.clouditora.mq.common.network.command.ClientRegisterCommand;
import com.clouditora.mq.common.network.command.ClientUnregisterCommand;
import com.clouditora.mq.common.network.command.MessagePullCommand;
import com.clouditora.mq.common.network.command.MessageSendCommand;
import com.clouditora.mq.common.topic.ConsumerSubscriptions;
import com.clouditora.mq.common.topic.ProducerGroup;
import com.clouditora.mq.common.topic.TopicQueue;
import com.clouditora.mq.common.util.EnumUtil;
import com.clouditora.mq.network.ClientNetwork;
import com.clouditora.mq.network.exception.ConnectException;
import com.clouditora.mq.network.exception.TimeoutException;
import com.clouditora.mq.network.protocol.Command;
import com.clouditora.mq.network.protocol.ResponseCode;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.Set;

@Slf4j
@Getter
public class BrokerApiFacade {
    /**
     * @link org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl#BROKER_SUSPEND_MAX_TIME_MILLIS
     */
    private static final long BROKER_SUSPEND_MAX_TIME_MILLIS = 1000 * 15;
    /**
     * @link org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl#CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND
     */
    private static final long CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND = 1000 * 30;

    private final ClientConfig clientConfig;
    private final ClientNetwork clientNetwork;

    public BrokerApiFacade(ClientConfig clientConfig, ClientNetwork clientNetwork) {
        this.clientConfig = clientConfig;
        this.clientNetwork = clientNetwork;
    }

    /**
     * @link org.apache.rocketmq.client.impl.MQClientAPIImpl#sendHeartbeat
     */
    public void heartbeat(String endpoint, String clientId, Set<ProducerGroup> producers, Set<ConsumerSubscriptions> consumers) throws InterruptedException, ConnectException, TimeoutException, BrokerException {
        ClientRegisterCommand.RequestBody requestBody = new ClientRegisterCommand.RequestBody();
        requestBody.setClientId(clientId);
        requestBody.setProducers(producers);
        requestBody.setConsumers(consumers);
        Command request = Command.buildRequest(RequestCode.REGISTER_CLIENT, null);
        request.setBody(requestBody.encode());

        Command response = this.clientNetwork.syncInvoke(endpoint, request, this.clientConfig.getMqClientApiTimeout());
        ResponseCode responseCode = EnumUtil.ofCode(response.getCode(), ResponseCode.class);
        if (responseCode == ResponseCode.SUCCESS) {
            log.info("heartbeat to broker: {}", endpoint);
            return;
        }
        throw new BrokerException(response.getCode(), response.getRemark(), endpoint);
    }

    /**
     * @link org.apache.rocketmq.client.impl.MQClientAPIImpl#unregisterClient
     */
    public void unregisterClient(String endpoint, String clientId, String producerGroup, String consumerGroup) throws InterruptedException, ConnectException, TimeoutException, BrokerException {
        ClientUnregisterCommand.RequestHeader requestHeader = new ClientUnregisterCommand.RequestHeader();
        requestHeader.setClientId(clientId);
        requestHeader.setProducerGroup(producerGroup);
        requestHeader.setConsumerGroup(consumerGroup);
        Command request = Command.buildRequest(RequestCode.UNREGISTER_CLIENT, requestHeader);

        Command response = this.clientNetwork.syncInvoke(endpoint, request, this.clientConfig.getMqClientApiTimeout());
        ResponseCode responseCode = EnumUtil.ofCode(response.getCode(), ResponseCode.class);
        if (responseCode == ResponseCode.SUCCESS) {
            log.info("unregister client: {}", endpoint);
            return;
        }
        throw new BrokerException(response.getCode(), response.getRemark(), endpoint);
    }

    public SendResult sendMessage(RpcModel rpcModel, String brokerName, String brokerEndpoint, String group, Message message, Integer queueId, long timeout) throws InterruptedException, ConnectException, TimeoutException, BrokerException {
        long startTime = System.currentTimeMillis();
        MessageSendCommand.RequestHeader requestHeader = new MessageSendCommand.RequestHeader();
        requestHeader.setGroup(group);
        requestHeader.setTopic(message.getTopic());
        requestHeader.setQueueId(queueId);
        requestHeader.setBornTimestamp(System.currentTimeMillis());
        requestHeader.setFlag(message.getFlag());
        requestHeader.setProperties(message.properties2String());
        Command request = Command.buildRequest(RequestCode.SEND_MESSAGE, requestHeader);

        if (rpcModel == RpcModel.SYNC) {
            Command response = this.clientNetwork.syncInvoke(brokerEndpoint, request, timeout);
            SendStatus sendStatus = parseSendStatus(brokerEndpoint, response);
            MessageSendCommand.ResponseHeader responseHeader = response.decodeHeader(MessageSendCommand.ResponseHeader.class);
            TopicQueue topicQueue = new TopicQueue();
            topicQueue.setTopic(message.getTopic());
            topicQueue.setBrokerName(brokerName);
            topicQueue.setQueueId(responseHeader.getQueueId());

            SendResult sendResult = new SendResult();
            sendResult.setStatus(sendStatus);
            sendResult.setMessageId(message.getProperty(MessageConst.Property.MESSAGE_ID));
            sendResult.setTopicQueue(topicQueue);
            sendResult.setQueueOffset(responseHeader.getQueueOffset());
            sendResult.setOffsetMessageId(responseHeader.getMessageId());
            return sendResult;
        } else if (rpcModel == RpcModel.ASYNC) {
            long elapsed = System.currentTimeMillis() - startTime;
            if (elapsed > timeout) {
                throw new TimeoutException(brokerEndpoint);
            }
            this.clientNetwork.asyncInvoke(brokerEndpoint, request, timeout - elapsed, null);
        } else if (rpcModel == RpcModel.ONEWAY) {
            this.clientNetwork.onewayInvoke(brokerEndpoint, request, timeout);
        }
        return null;
    }

    public PullResult pullMessage(RpcModel rpcModel, String endpoint) throws InterruptedException, ConnectException, TimeoutException, BrokerException {
        long startTime = System.currentTimeMillis();
        MessagePullCommand.RequestHeader requestHeader = new MessagePullCommand.RequestHeader();
        requestHeader.setGroup();
        requestHeader.setTopic();
        requestHeader.setQueueId();
        requestHeader.setQueueOffset();
        requestHeader.setMaxNumber();
        requestHeader.setSysFlag();
        requestHeader.setCommitOffset();
        requestHeader.setSuspendTimeout(BROKER_SUSPEND_MAX_TIME_MILLIS);
        requestHeader.setSubscription();
        requestHeader.setSubVersion();
        requestHeader.setExpressionType();
        Command request = Command.buildRequest(RequestCode.UNREGISTER_CLIENT, requestHeader);

        if (rpcModel == RpcModel.SYNC) {
            Command response = this.clientNetwork.syncInvoke(endpoint, request, CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND);
            PullStatus pullStatus = parsePullStatus(endpoint, response);
            MessagePullCommand.ResponseHeader responseHeader = response.decodeHeader(MessagePullCommand.ResponseHeader.class);
            PullResult pullResult = new PullResult();
            pullResult.setStatus(pullStatus);
            pullResult.setNextBeginOffset(responseHeader.getNextBeginOffset());
            pullResult.setMinOffset(responseHeader.getMinOffset());
            pullResult.setMaxOffset(responseHeader.getMaxOffset());
            pullResult.setMessages(null);
            pullResult.setSuggestWhichBrokerId(responseHeader.getSuggestWhichBrokerId());
            pullResult.setMessageBinary(response.getBody());
            return pullResult;
        } else if (rpcModel == RpcModel.ASYNC) {
            long elapsed = System.currentTimeMillis() - startTime;
            if (elapsed > CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND) {
                throw new TimeoutException(endpoint);
            }
            this.clientNetwork.asyncInvoke(endpoint, request, CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND - elapsed, null);
        } else if (rpcModel == RpcModel.ONEWAY) {
            this.clientNetwork.onewayInvoke(endpoint, request, CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND);
        }
        return null;
    }

    private static SendStatus parseSendStatus(String endpoint, Command response) throws BrokerException {
        SendStatus status;
        switch (EnumUtil.ofCode(response.getCode(), ResponseCode.class)) {
            case SUCCESS -> status = SendStatus.SEND_OK;
            case FLUSH_DISK_TIMEOUT -> status = SendStatus.FLUSH_DISK_TIMEOUT;
            case FLUSH_SLAVE_TIMEOUT -> status = SendStatus.FLUSH_SLAVE_TIMEOUT;
            case SLAVE_NOT_AVAILABLE -> status = SendStatus.SLAVE_NOT_AVAILABLE;
            default -> throw new BrokerException(response.getCode(), response.getRemark(), endpoint);
        }
        return status;
    }

    private static PullStatus parsePullStatus(String endpoint, Command response) throws BrokerException {
        PullStatus status;
        switch (EnumUtil.ofCode(response.getCode(), ResponseCode.class)) {
            case SUCCESS -> status = PullStatus.FOUND;
            case PULL_NOT_FOUND -> status = PullStatus.NO_NEW_MSG;
            case PULL_RETRY_IMMEDIATELY -> status = PullStatus.NO_MATCHED_MSG;
            case PULL_OFFSET_MOVED -> status = PullStatus.OFFSET_ILLEGAL;
            default -> throw new BrokerException(response.getCode(), response.getRemark(), endpoint);
        }
        return status;
    }
}
