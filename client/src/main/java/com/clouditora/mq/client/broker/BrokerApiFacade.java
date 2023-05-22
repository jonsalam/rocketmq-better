package com.clouditora.mq.client.broker;

import com.clouditora.mq.client.instance.ClientConfig;
import com.clouditora.mq.common.Message;
import com.clouditora.mq.common.MessageConst;
import com.clouditora.mq.common.constant.RpcModel;
import com.clouditora.mq.common.constant.SendStatus;
import com.clouditora.mq.common.exception.BrokerException;
import com.clouditora.mq.common.message.MessageQueue;
import com.clouditora.mq.common.message.SendResult;
import com.clouditora.mq.common.network.RequestCode;
import com.clouditora.mq.common.network.command.ClientRegisterCommand;
import com.clouditora.mq.common.network.command.ClientUnregisterCommand;
import com.clouditora.mq.common.network.command.MessageSendCommand;
import com.clouditora.mq.common.topic.ConsumerSubscriptions;
import com.clouditora.mq.common.topic.ProducerGroup;
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

    public SendResult sendMessage(RpcModel rpcModel, String endpoint, String group, Message message, Integer queueId, String brokerName, long timeout) throws InterruptedException, ConnectException,  TimeoutException, BrokerException {
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
            Command response = this.clientNetwork.syncInvoke(endpoint, request, timeout);
            ResponseCode responseCode = EnumUtil.ofCode(response.getCode(), ResponseCode.class);
            SendStatus sendStatus = parseSendStatus(endpoint, response, responseCode);
            MessageSendCommand.ResponseHeader responseHeader = response.decodeHeader(MessageSendCommand.ResponseHeader.class);
            MessageQueue messageQueue = new MessageQueue();
            messageQueue.setTopic(message.getTopic());
            messageQueue.setBrokerName(brokerName);
            messageQueue.setQueueId(responseHeader.getQueueId());

            SendResult sendResult = new SendResult();
            sendResult.setSendStatus(sendStatus);
            sendResult.setMsgId(message.getProperty(MessageConst.Property.MESSAGE_ID));
            sendResult.setMessageQueue(messageQueue);
            sendResult.setQueueOffset(responseHeader.getQueueOffset());
            sendResult.setOffsetMsgId(responseHeader.getMsgId());
            return sendResult;
        } else if (rpcModel == RpcModel.ASYNC) {
            long elapsed = System.currentTimeMillis() - startTime;
            if (elapsed > timeout) {
                throw new TimeoutException(endpoint);
            }
            this.clientNetwork.asyncInvoke(endpoint, request, timeout - elapsed, null);
        } else if (rpcModel == RpcModel.ONEWAY) {
            this.clientNetwork.onewayInvoke(endpoint, request, timeout);
        }
        return null;
    }

    private static SendStatus parseSendStatus(String endpoint, Command response, ResponseCode responseCode) throws BrokerException {
        SendStatus sendStatus;
        switch (responseCode) {
            case SUCCESS -> sendStatus = SendStatus.SEND_OK;
            case FLUSH_DISK_TIMEOUT -> sendStatus = SendStatus.FLUSH_DISK_TIMEOUT;
            case FLUSH_SLAVE_TIMEOUT -> sendStatus = SendStatus.FLUSH_SLAVE_TIMEOUT;
            case SLAVE_NOT_AVAILABLE -> sendStatus = SendStatus.SLAVE_NOT_AVAILABLE;
            default -> throw new BrokerException(response.getCode(), response.getRemark(), endpoint);
        }
        return sendStatus;
    }
}
