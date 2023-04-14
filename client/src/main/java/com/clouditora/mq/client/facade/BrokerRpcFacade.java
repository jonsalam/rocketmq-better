package com.clouditora.mq.client.facade;

import com.clouditora.mq.client.exception.MqClientException;
import com.clouditora.mq.common.command.RequestCode;
import com.clouditora.mq.common.command.body.TopicRouteData;
import com.clouditora.mq.common.command.header.CreateTopicRequestHeader;
import com.clouditora.mq.common.command.header.GetRouteInfoRequestHeader;
import com.clouditora.mq.common.topic.TopicConfig;
import com.clouditora.mq.common.util.EnumUtil;
import com.clouditora.mq.network.Client;
import com.clouditora.mq.network.exception.ConnectException;
import com.clouditora.mq.network.exception.TimeoutException;
import com.clouditora.mq.network.protocol.Command;
import com.clouditora.mq.network.protocol.ResponseCode;
import lombok.extern.slf4j.Slf4j;

/**
 * org.apache.rocketmq.client.impl.MQClientAPIImpl
 */
@Slf4j
public class BrokerRpcFacade {
    private Client client;

    /**
     * org.apache.rocketmq.client.impl.MQClientAPIImpl#getTopicRouteInfoFromNameServer
     */
    public TopicRouteData getTopicRouteInfoFromNameServer(String topic, long timeout) throws MqClientException, InterruptedException, TimeoutException, ConnectException {
        GetRouteInfoRequestHeader requestHeader = new GetRouteInfoRequestHeader();
        requestHeader.setTopic(topic);

        Command request = Command.buildRequest(RequestCode.GET_ROUTEINFO_BY_TOPIC, requestHeader);
        Command response = this.client.syncInvoke(null, request, timeout);
        switch (EnumUtil.ofCode(ResponseCode.class, response.getCode())) {
            case TOPIC_NOT_EXIST: {
                log.warn("topic {} not exists", topic);
                break;
            }
            case SUCCESS: {
                byte[] body = response.getBody();
                if (body != null) {
                    return TopicRouteData.decode(body, TopicRouteData.class);
                }
            }
            default:
                break;
        }
        throw new MqClientException(response.getCode(), response.getRemark());
    }

    /**
     * @link org.apache.rocketmq.client.impl.MQClientAPIImpl#createTopic
     */
    public void createTopic(String address, String defaultTopic, TopicConfig topicConfig, long timeout) throws MqClientException, InterruptedException, TimeoutException, ConnectException {
        CreateTopicRequestHeader requestHeader = new CreateTopicRequestHeader();
        requestHeader.setTopic(topicConfig.getTopicName());
        requestHeader.setDefaultTopic(defaultTopic);
        requestHeader.setReadQueueNums(topicConfig.getReadQueueNums());
        requestHeader.setWriteQueueNums(topicConfig.getWriteQueueNums());
        requestHeader.setPerm(topicConfig.getPerm());
        requestHeader.setTopicFilterType(topicConfig.getTopicFilterType().name());
        requestHeader.setTopicSysFlag(topicConfig.getTopicSysFlag());
        requestHeader.setOrder(topicConfig.isOrder());

        Command request = Command.buildRequest(RequestCode.UPDATE_AND_CREATE_TOPIC, requestHeader);
        Command response = this.client.syncInvoke(address, request, timeout);
        if (EnumUtil.ofCode(ResponseCode.class, response.getCode()) != ResponseCode.SUCCESS) {
            throw new MqClientException(response.getCode(), response.getRemark());
        }
        log.info("topic {} create success", topicConfig.getTopicName());
    }
}
