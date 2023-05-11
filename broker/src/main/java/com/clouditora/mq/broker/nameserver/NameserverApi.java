package com.clouditora.mq.broker.nameserver;

import com.clouditora.mq.common.command.RequestCode;
import com.clouditora.mq.common.command.protocol.BrokerRegisterCommand;
import com.clouditora.mq.common.command.protocol.BrokerUnregisterCommand;
import com.clouditora.mq.network.Client;
import com.clouditora.mq.network.exception.ConnectException;
import com.clouditora.mq.network.exception.TimeoutException;
import com.clouditora.mq.network.protocol.Command;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
public class NameserverApi {
    private final Client client;

    public NameserverApi(Client client) {
        this.client = client;
    }

    public void registerBroker(String endpoint, BrokerRegisterCommand.RequestHeader requestHeader, long timeout) throws InterruptedException, TimeoutException, ConnectException {
        Command request= Command.buildRequest(RequestCode.REGISTER_BROKER, requestHeader);
        Command response = client.syncInvoke(endpoint, request, timeout);
        log.info("register broker: response={}", response);
    }

    public void unregisterBroker(String endpoint, BrokerUnregisterCommand.RequestHeader requestHeader, int timeout) throws InterruptedException, TimeoutException, ConnectException {
        Command request= Command.buildRequest(RequestCode.UNREGISTER_BROKER, requestHeader);
        Command response = client.syncInvoke(endpoint, request, timeout);
        log.info("unregister broker: response={}", response);
    }
}
