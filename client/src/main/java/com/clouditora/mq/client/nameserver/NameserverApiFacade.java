package com.clouditora.mq.client.nameserver;

import com.clouditora.mq.client.instance.ClientConfig;
import com.clouditora.mq.common.command.RequestCode;
import com.clouditora.mq.common.command.protocol.TopicRouteCommand;
import com.clouditora.mq.common.exception.ClientException;
import com.clouditora.mq.common.util.EnumUtil;
import com.clouditora.mq.network.Client;
import com.clouditora.mq.network.exception.ConnectException;
import com.clouditora.mq.network.exception.TimeoutException;
import com.clouditora.mq.network.protocol.Command;
import com.clouditora.mq.network.protocol.ResponseCode;

public class NameserverApiFacade {
    private final Client client;
    private final ClientConfig clientConfig;

    public NameserverApiFacade(Client client, ClientConfig clientConfig) {
        this.client = client;
        this.clientConfig = clientConfig;
    }

    /**
     * org.apache.rocketmq.client.impl.MQClientAPIImpl#getTopicRouteInfoFromNameServer
     */
    public TopicRouteCommand.ResponseBody getTopicRoute(String topic) throws InterruptedException, TimeoutException, ConnectException, ClientException {
        TopicRouteCommand.RequestHeader requestHeader = new TopicRouteCommand.RequestHeader();
        requestHeader.setTopic(topic);
        Command request = Command.buildRequest(RequestCode.GET_ROUTEINFO_BY_TOPIC, requestHeader);
        Command response = this.client.syncInvoke(null, request, this.clientConfig.getMqClientApiTimeout());
        ResponseCode responseCode = EnumUtil.ofCode(response.getCode(), ResponseCode.class);
        if (responseCode == ResponseCode.SUCCESS) {
            return response.decodeBody(TopicRouteCommand.ResponseBody.class);
        }
        throw new ClientException(response.getCode(), response.getRemark());
    }
}
