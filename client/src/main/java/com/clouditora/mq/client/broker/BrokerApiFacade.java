package com.clouditora.mq.client.broker;

import com.clouditora.mq.common.exception.BrokerException;
import com.clouditora.mq.common.command.RequestCode;
import com.clouditora.mq.common.command.protocol.ClientHeartBeatCommand;
import com.clouditora.mq.common.util.EnumUtil;
import com.clouditora.mq.network.Client;
import com.clouditora.mq.network.exception.ConnectException;
import com.clouditora.mq.network.exception.TimeoutException;
import com.clouditora.mq.network.protocol.Command;
import com.clouditora.mq.network.protocol.ResponseCode;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
public class BrokerApiFacade {
    private final Client client;

    public BrokerApiFacade(Client client) {
        this.client = client;
    }

    public void heartbeat(String endpoint, ClientHeartBeatCommand.RequestBody requestBody, long timeout) throws InterruptedException, TimeoutException, ConnectException, BrokerException {
        Command request = Command.buildRequest(RequestCode.HEART_BEAT, null);
        request.setBody(requestBody.encode());
        Command response = this.client.syncInvoke(endpoint, request, timeout);
        ResponseCode responseCode = EnumUtil.ofCode(response.getCode(), ResponseCode.class);
        if(responseCode == ResponseCode.SUCCESS){
            log.info("heartbeat to broker {}", endpoint);
        }
        throw new BrokerException(response.getCode(), response.getRemark(), endpoint);
    }
}
