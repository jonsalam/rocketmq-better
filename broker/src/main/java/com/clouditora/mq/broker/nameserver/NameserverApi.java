package com.clouditora.mq.broker.nameserver;

import com.clouditora.mq.common.constant.RpcModel;
import com.clouditora.mq.common.network.RequestCode;
import com.clouditora.mq.common.network.command.BrokerRegisterCommand;
import com.clouditora.mq.common.network.command.BrokerUnregisterCommand;
import com.clouditora.mq.network.ClientNetwork;
import com.clouditora.mq.network.exception.ConnectException;
import com.clouditora.mq.network.exception.TimeoutException;
import com.clouditora.mq.network.protocol.Command;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
public class NameserverApi {
    private final ClientNetwork clientNetwork;

    public NameserverApi(ClientNetwork clientNetwork) {
        this.clientNetwork = clientNetwork;
    }

    /**
     * @link org.apache.rocketmq.broker.out.BrokerOuterAPI#registerBroker
     */
    public void registerBroker(RpcModel rpcModel, String endpoint, BrokerRegisterCommand.RequestHeader requestHeader, BrokerRegisterCommand.RequestBody requestBody, long timeout) throws InterruptedException, TimeoutException, ConnectException {
        Command request = Command.buildRequest(RequestCode.REGISTER_BROKER, requestHeader);
        request.setBody(requestBody.encode());
        if (rpcModel == RpcModel.SYNC) {
            Command response = clientNetwork.syncInvoke(endpoint, request, timeout);
            log.info("register broker: response={}", response);
        } else if (rpcModel == RpcModel.ASYNC) {
            clientNetwork.asyncInvoke(endpoint, request, timeout, commandFuture -> {
                log.info("register broker: response={}", commandFuture.getCommand());
            });
        } else if (rpcModel == RpcModel.ONEWAY) {
            clientNetwork.onewayInvoke(endpoint, request, timeout);
        }
    }

    /**
     * @link org.apache.rocketmq.broker.out.BrokerOuterAPI#unregisterBroker
     */
    public void unregisterBroker(String endpoint, BrokerUnregisterCommand.RequestHeader requestHeader, int timeout) throws InterruptedException, TimeoutException, ConnectException {
        Command request = Command.buildRequest(RequestCode.UNREGISTER_BROKER, requestHeader);
        Command response = clientNetwork.syncInvoke(endpoint, request, timeout);
        log.info("unregister broker: response={}", response);
    }
}
