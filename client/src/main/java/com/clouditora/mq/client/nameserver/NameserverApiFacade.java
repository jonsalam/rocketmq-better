package com.clouditora.mq.client.nameserver;

import com.clouditora.mq.client.instance.ClientConfig;
import com.clouditora.mq.common.exception.ClientException;
import com.clouditora.mq.common.network.RequestCode;
import com.clouditora.mq.common.network.command.TopicRouteCommand;
import com.clouditora.mq.common.topic.TopicRoute;
import com.clouditora.mq.common.util.EnumUtil;
import com.clouditora.mq.network.ClientNetwork;
import com.clouditora.mq.network.exception.ConnectException;
import com.clouditora.mq.network.exception.TimeoutException;
import com.clouditora.mq.network.protocol.Command;
import com.clouditora.mq.network.protocol.ResponseCode;

public class NameserverApiFacade {
    private final ClientConfig clientConfig;
    private final ClientNetwork clientNetwork;

    public NameserverApiFacade(ClientConfig clientConfig, ClientNetwork clientNetwork) {
        this.clientConfig = clientConfig;
        this.clientNetwork = clientNetwork;
    }

    /**
     * org.apache.rocketmq.client.impl.MQClientAPIImpl#getTopicRouteInfoFromNameServer
     */
    public TopicRoute getTopicRoute(String topic) throws InterruptedException, TimeoutException, ConnectException, ClientException {
        TopicRouteCommand.RequestHeader requestHeader = new TopicRouteCommand.RequestHeader();
        requestHeader.setTopic(topic);
        Command request = Command.buildRequest(RequestCode.GET_TOPIC_ROUTE_BY_TOPIC, requestHeader);
        Command response = this.clientNetwork.syncInvoke(null, request, this.clientConfig.getMqClientApiTimeout());
        ResponseCode responseCode = EnumUtil.ofCode(response.getCode(), ResponseCode.class);
        if (responseCode == ResponseCode.SUCCESS) {
            return response.decodeBody(TopicRouteCommand.ResponseBody.class);
        }
        throw new ClientException(response.getCode(), response.getRemark());
    }
}
