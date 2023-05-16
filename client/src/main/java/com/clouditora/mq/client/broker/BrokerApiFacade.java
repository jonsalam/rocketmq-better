package com.clouditora.mq.client.broker;

import com.clouditora.mq.client.instance.ClientConfig;
import com.clouditora.mq.common.exception.BrokerException;
import com.clouditora.mq.common.network.RequestCode;
import com.clouditora.mq.common.network.command.ClientRegisterCommand;
import com.clouditora.mq.common.network.command.ClientUnregisterCommand;
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
    public void heartbeat(String endpoint, String clientId, Set<ProducerGroup> producers, Set<ConsumerSubscriptions> consumers) throws InterruptedException, TimeoutException, ConnectException, BrokerException {
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
    public void unregisterClient(String endpoint, String clientId, String producerGroup, String consumerGroup) throws InterruptedException, TimeoutException, ConnectException, BrokerException {
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
}
