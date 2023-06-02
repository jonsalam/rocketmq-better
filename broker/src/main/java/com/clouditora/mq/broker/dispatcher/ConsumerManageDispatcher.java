package com.clouditora.mq.broker.dispatcher;

import com.clouditora.mq.broker.client.ConsumerManager;
import com.clouditora.mq.common.network.RequestCode;
import com.clouditora.mq.common.network.command.ConsumerFindCommand;
import com.clouditora.mq.common.util.EnumUtil;
import com.clouditora.mq.network.command.CommandDispatcher;
import com.clouditora.mq.network.protocol.Command;
import com.clouditora.mq.network.protocol.ResponseCode;
import com.clouditora.mq.network.util.NetworkUtil;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * @link org.apache.rocketmq.broker.processor.ConsumerManageProcessor
 */
@Slf4j
public class ConsumerManageDispatcher implements CommandDispatcher {
    private final ConsumerManager consumerManager;

    public ConsumerManageDispatcher(ConsumerManager consumerManager) {
        this.consumerManager = consumerManager;
    }

    @Override
    public Command request(ChannelHandlerContext context, Command request) throws Exception {
        RequestCode requestCode = EnumUtil.ofCode(request.getCode(), RequestCode.class);
        switch (requestCode) {
            case GET_CONSUMER_LIST_BY_GROUP -> {
                return findConsumerIdsByGroup(context, request);
            }
            default -> {
                log.error("{} request code [{}] is not supported", NetworkUtil.toEndpoint(context.channel()), request.getCode());
                return null;
            }
        }
    }

    private Command findConsumerIdsByGroup(ChannelHandlerContext context, Command request) {
        ConsumerFindCommand.RequestHeader requestHeader = request.decodeHeader(ConsumerFindCommand.RequestHeader.class);
        List<String> clientIds =  this.consumerManager.findConsumerIdsByGroup(requestHeader.getGroup());
        ConsumerFindCommand.ResponseBody responseBody = new ConsumerFindCommand.ResponseBody();
        responseBody.setConsumerIds(clientIds);
        Command response = Command.buildResponse(ResponseCode.SUCCESS);
        response.setBody(responseBody);
        return response;
    }
}
