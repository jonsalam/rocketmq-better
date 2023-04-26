package com.clouditora.mq.nameserver.processor;

import com.clouditora.mq.common.command.RequestCode;
import com.clouditora.mq.common.command.protocol.BrokerRegister;
import com.clouditora.mq.common.command.protocol.BrokerUnregister;
import com.clouditora.mq.common.util.EnumUtil;
import com.clouditora.mq.nameserver.route.RouteInfoManager;
import com.clouditora.mq.network.CommandRequestProcessor;
import com.clouditora.mq.network.protocol.Command;
import com.clouditora.mq.network.util.CoordinatorUtil;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

/**
 * @link org.apache.rocketmq.namesrv.processor.DefaultRequestProcessor
 */
@Slf4j
public class DefaultRequestProcessor implements CommandRequestProcessor {
    protected final RouteInfoManager routeInfoManager;

    public DefaultRequestProcessor(RouteInfoManager routeInfoManager) {
        this.routeInfoManager = routeInfoManager;
    }

    @Override
    public Command process(ChannelHandlerContext context, Command request) throws Exception {
        RequestCode requestCode = EnumUtil.ofCode(RequestCode.class, request.getCode());
        switch (requestCode) {
            case REGISTER_BROKER -> {
                return registerBrokerWithFilterServer(context, request);
            }
            case UNREGISTER_BROKER -> {
                return unregisterBroker(context, request);
            }
            default -> {
                log.error("{} request code [{}] is not supported", CoordinatorUtil.toEndpoint(context.channel()), request.getCode());
                return null;
            }
        }
    }

    /**
     * @link org.apache.rocketmq.namesrv.processor.DefaultRequestProcessor#registerBrokerWithFilterServer
     */
    private Command registerBrokerWithFilterServer(ChannelHandlerContext context, Command request) {
        BrokerRegister.RequestHeader requestHeader = request.decodeHeader(BrokerRegister.RequestHeader.class);
        this.routeInfoManager.registerBroker(
                requestHeader.getClusterName(),
                requestHeader.getBrokerName(),
                requestHeader.getBrokerEndpoint(),
                requestHeader.getBrokerId(),
                context.channel()
        );
        return Command.buildResponse();
    }

    /**
     * @link org.apache.rocketmq.namesrv.processor.DefaultRequestProcessor#unregisterBroker
     */
    private Command unregisterBroker(ChannelHandlerContext context, Command request) {
        BrokerUnregister.RequestHeader requestHeader = request.decodeHeader(BrokerUnregister.RequestHeader.class);
        this.routeInfoManager.unregisterBroker(
                requestHeader.getClusterName(),
                requestHeader.getBrokerName(),
                requestHeader.getBrokerEndpoint(),
                requestHeader.getBrokerId()
        );
        return Command.buildResponse();
    }
}
