package com.clouditora.mq.nameserver.dispatcher;

import com.clouditora.mq.common.network.RequestCode;
import com.clouditora.mq.common.network.command.BrokerRegisterCommand;
import com.clouditora.mq.common.network.command.BrokerUnregisterCommand;
import com.clouditora.mq.common.util.EnumUtil;
import com.clouditora.mq.nameserver.route.TopicRouteManager;
import com.clouditora.mq.network.command.CommandDispatcher;
import com.clouditora.mq.network.protocol.Command;
import com.clouditora.mq.network.util.NetworkUtil;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

/**
 * @link org.apache.rocketmq.namesrv.processor.DefaultRequestProcessor
 */
@Slf4j
public class DefaultCommandDispatcher implements CommandDispatcher {
    protected final TopicRouteManager topicRouteManager;

    public DefaultCommandDispatcher(TopicRouteManager topicRouteManager) {
        this.topicRouteManager = topicRouteManager;
    }

    @Override
    public Command request(ChannelHandlerContext context, Command request) throws Exception {
        RequestCode requestCode = EnumUtil.ofCode(request.getCode(), RequestCode.class);
        switch (requestCode) {
            case REGISTER_BROKER -> {
                return registerBroker(context, request);
            }
            case UNREGISTER_BROKER -> {
                return unregisterBroker(context, request);
            }
            default -> {
                log.error("{} request code [{}] is not supported", NetworkUtil.toEndpoint(context.channel()), request.getCode());
                return null;
            }
        }
    }

    /**
     * @link org.apache.rocketmq.namesrv.processor.DefaultRequestProcessor#registerBrokerWithFilterServer
     */
    private Command registerBroker(ChannelHandlerContext context, Command request) {
        BrokerRegisterCommand.RequestHeader requestHeader = request.decodeHeader(BrokerRegisterCommand.RequestHeader.class);
        BrokerRegisterCommand.RequestBody requestBody = request.decodeBody(BrokerRegisterCommand.RequestBody.class);
        this.topicRouteManager.registerBroker(
                requestHeader.getClusterName(),
                requestHeader.getBrokerName(),
                requestHeader.getBrokerEndpoint(),
                requestHeader.getBrokerId(),
                requestBody.getTopicMap(),
                context.channel()
        );
        return Command.buildResponse();
    }

    /**
     * @link org.apache.rocketmq.namesrv.processor.DefaultRequestProcessor#unregisterBroker
     */
    private Command unregisterBroker(ChannelHandlerContext context, Command request) {
        BrokerUnregisterCommand.RequestHeader requestHeader = request.decodeHeader(BrokerUnregisterCommand.RequestHeader.class);
        this.topicRouteManager.unregisterBroker(
                requestHeader.getClusterName(),
                requestHeader.getBrokerName(),
                requestHeader.getBrokerEndpoint(),
                requestHeader.getBrokerId()
        );
        return Command.buildResponse();
    }
}
