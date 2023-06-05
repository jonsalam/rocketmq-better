package com.clouditora.mq.client.broker;

import com.clouditora.mq.client.consumer.pull.PullMessageCallback;
import com.clouditora.mq.client.consumer.pull.PullResult;
import com.clouditora.mq.client.consumer.pull.PullStatus;
import com.clouditora.mq.client.instance.ClientConfig;
import com.clouditora.mq.client.producer.SendResult;
import com.clouditora.mq.client.producer.SendStatus;
import com.clouditora.mq.common.Message;
import com.clouditora.mq.common.MessageConst;
import com.clouditora.mq.common.constant.RpcModel;
import com.clouditora.mq.common.exception.BrokerException;
import com.clouditora.mq.common.exception.ClientException;
import com.clouditora.mq.common.network.RequestCode;
import com.clouditora.mq.common.network.command.*;
import com.clouditora.mq.common.topic.GroupSubscription;
import com.clouditora.mq.common.topic.ProducerGroup;
import com.clouditora.mq.common.topic.TopicQueue;
import com.clouditora.mq.common.topic.TopicSubscription;
import com.clouditora.mq.common.util.EnumUtil;
import com.clouditora.mq.network.ClientNetwork;
import com.clouditora.mq.network.exception.ConnectException;
import com.clouditora.mq.network.exception.TimeoutException;
import com.clouditora.mq.network.protocol.Command;
import com.clouditora.mq.network.protocol.ResponseCode;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
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
    public void heartbeat(String endpoint, String clientId, Set<ProducerGroup> producers, Set<GroupSubscription> consumers) throws InterruptedException, ConnectException, TimeoutException, BrokerException {
        ClientRegisterCommand.RequestBody requestBody = new ClientRegisterCommand.RequestBody();
        requestBody.setClientId(clientId);
        requestBody.setProducers(producers);
        requestBody.setConsumers(consumers);
        Command request = Command.buildRequest(RequestCode.REGISTER_CLIENT, null);
        request.setBody(requestBody);

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

    /**
     * @link org.apache.rocketmq.client.impl.MQClientAPIImpl#lockBatchMQ
     */
    public Set<TopicQueue> lockQueue(String endpoint, String group, Set<TopicQueue> queues, String clientId) throws InterruptedException, ConnectException, TimeoutException, BrokerException {
        LockQueueCommand.RequestBody requestBody = new LockQueueCommand.RequestBody();
        requestBody.setGroup(group);
        requestBody.setQueues(queues);
        requestBody.setClientId(clientId);
        Command request = Command.buildRequest(RequestCode.LOCK_BATCH_MQ, null);
        request.setBody(requestBody);
        Command response = this.clientNetwork.syncInvoke(endpoint, request, 1000);
        ResponseCode responseCode = EnumUtil.ofCode(response.getCode(), ResponseCode.class);
        if (responseCode == ResponseCode.SUCCESS) {
            log.info("unlock queue to broker: {}", endpoint);
            LockQueueCommand.ResponseBody responseBody = response.decodeBody(LockQueueCommand.ResponseBody.class);
            return responseBody.getQueues();
        }
        throw new BrokerException(responseCode.getCode(), response.getRemark(), endpoint);
    }

    /**
     * @link org.apache.rocketmq.client.impl.MQClientAPIImpl#unlockBatchMQ
     */
    public void unlockQueue(boolean oneway, String endpoint, String group, Set<TopicQueue> queues, String clientId) throws InterruptedException, ConnectException, TimeoutException, BrokerException {
        UnlockQueueCommand.RequestBody requestBody = new UnlockQueueCommand.RequestBody();
        requestBody.setGroup(group);
        requestBody.setQueues(queues);
        requestBody.setClientId(clientId);
        Command request = Command.buildRequest(RequestCode.UNLOCK_BATCH_MQ, null);
        request.setBody(requestBody);
        if (oneway) {
            this.clientNetwork.onewayInvoke(endpoint, request, 1000);
        } else {
            Command response = this.clientNetwork.syncInvoke(endpoint, request, 1000);
            ResponseCode responseCode = EnumUtil.ofCode(response.getCode(), ResponseCode.class);
            if (responseCode == ResponseCode.SUCCESS) {
                log.info("unlock queue to broker: {}", endpoint);
                return;
            }
            throw new BrokerException(responseCode.getCode(), response.getRemark(), endpoint);
        }
    }

    /**
     * @link org.apache.rocketmq.client.impl.MQClientAPIImpl#getConsumerIdListByGroup
     */
    public List<String> findConsumerIdsByGroup(String endpoint, String group) throws InterruptedException, ConnectException, TimeoutException, BrokerException {
        ConsumerFindCommand.RequestHeader requestHeader = new ConsumerFindCommand.RequestHeader();
        requestHeader.setGroup(group);
        Command request = Command.buildRequest(RequestCode.GET_CONSUMER_LIST_BY_GROUP, requestHeader);
        Command response = this.clientNetwork.syncInvoke(endpoint, request, this.clientConfig.getMqClientApiTimeout());
        ResponseCode responseCode = EnumUtil.ofCode(response.getCode(), ResponseCode.class);
        if (responseCode == ResponseCode.SUCCESS) {
            ConsumerFindCommand.ResponseBody responseBody = response.decodeBody(ConsumerFindCommand.ResponseBody.class);
            log.info("find consumers: {} {}", endpoint, responseBody.getConsumerIds());
            return responseBody.getConsumerIds();
        }
        throw new BrokerException(responseCode.getCode(), response.getRemark(), endpoint);
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

    /**
     * @link org.apache.rocketmq.client.impl.MQClientAPIImpl#pullMessage
     */
    public PullResult syncPullMessage(String endpoint, String group, TopicQueue topicQueue, TopicSubscription subscription, long pullOffset, long commitOffset, int sysFlag, int pullNum) throws InterruptedException, ConnectException, TimeoutException, BrokerException {
        Command request = buildPullRequest(group, topicQueue, subscription, pullOffset, commitOffset, sysFlag, pullNum);
        Command response = this.clientNetwork.syncInvoke(endpoint, request, CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND);
        return parsePullResponse(endpoint, response);
    }

    /**
     * @link org.apache.rocketmq.client.impl.MQClientAPIImpl#pullMessageAsync
     */
    public void asyncPullMessage(String endpoint, String group, TopicQueue topicQueue, TopicSubscription subscription, long pullOffset, long commitOffset, int sysFlag, int pullNum, PullMessageCallback callback) throws InterruptedException, ConnectException, TimeoutException, BrokerException {
        long startTime = System.currentTimeMillis();
        Command request = buildPullRequest(group, topicQueue, subscription, pullOffset, commitOffset, sysFlag, pullNum);
        long elapsed = System.currentTimeMillis() - startTime;
        if (elapsed > CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND) {
            throw new TimeoutException(endpoint);
        }
        this.clientNetwork.asyncInvoke(endpoint, request, CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND - elapsed, f -> {
            Command response = f.getResponse();
            if (response == null) {
                if (!f.isSendOk()) {
                    callback.onException(new ClientException("send request to %s".formatted(endpoint), f.getCause()));
                } else if (f.isTimeout()) {
                    callback.onException(new ClientException("wait response timeout from %s".formatted(endpoint), f.getCause()));
                } else {
                    callback.onException(new ClientException("unknown reason to %s".formatted(endpoint), f.getCause()));
                }
            } else {
                try {
                    PullResult result = parsePullResponse(endpoint, response);
                    callback.onSuccess(result);
                } catch (Exception e) {
                    callback.onException(e);
                }
            }
        });
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

    private static Command buildPullRequest(String group, TopicQueue topicQueue, TopicSubscription subscription, long pullOffset, long commitOffset, int sysFlag, int pullNum) {
        MessagePullCommand.RequestHeader requestHeader = new MessagePullCommand.RequestHeader();
        requestHeader.setGroup(group);
        requestHeader.setTopic(topicQueue.getTopic());
        requestHeader.setQueueId(topicQueue.getQueueId());
        requestHeader.setPullNum(pullNum);
        requestHeader.setPullOffset(pullOffset);
        requestHeader.setCommitOffset(commitOffset);
        requestHeader.setSysFlag(sysFlag);
        requestHeader.setSuspendTimeout(BROKER_SUSPEND_MAX_TIME_MILLIS);
        requestHeader.setExpression(subscription.getExpression());
        requestHeader.setExpressionType(subscription.getExpressionType());
        requestHeader.setVersion(subscription.getVersion());
        return Command.buildRequest(RequestCode.PULL_MESSAGE, requestHeader);
    }

    private static PullResult parsePullResponse(String endpoint, Command response) throws BrokerException {
        MessagePullCommand.ResponseHeader responseHeader = response.decodeHeader(MessagePullCommand.ResponseHeader.class);
        PullResult result = new PullResult();
        result.setStatus(parsePullResponseStatus(endpoint, response));
        result.setNextBeginOffset(responseHeader.getNextBeginOffset());
        result.setMinOffset(responseHeader.getMinOffset());
        result.setMaxOffset(responseHeader.getMaxOffset());
        result.setMessages(null);
        result.setMessageBinary(response.getBody());
        return result;
    }

    private static PullStatus parsePullResponseStatus(String endpoint, Command response) throws BrokerException {
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
