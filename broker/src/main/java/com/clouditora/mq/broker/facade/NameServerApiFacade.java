package com.clouditora.mq.broker.facade;

import com.clouditora.mq.common.command.BrokerRegister;
import com.clouditora.mq.common.command.RequestCode;
import com.clouditora.mq.network.Client;
import com.clouditora.mq.network.exception.ConnectException;
import com.clouditora.mq.network.exception.TimeoutException;
import com.clouditora.mq.network.protocol.Command;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NameServerApiFacade {
    private final Client client;

    public NameServerApiFacade(Client client) {
        this.client = client;
    }

    public void registerBroker(String address, BrokerRegister.RequestHeader requestHeader, long timeout) throws InterruptedException, TimeoutException, ConnectException {
        Command request= Command.buildRequest(RequestCode.REGISTER_BROKER, requestHeader);
        Command response = client.syncInvoke(address, request, timeout);
        log.info("register broker: response={}", response);
    }
}
