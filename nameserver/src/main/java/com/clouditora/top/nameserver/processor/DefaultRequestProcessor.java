package com.clouditora.top.nameserver.processor;

import com.clouditora.mq.common.command.RequestCode;
import com.clouditora.mq.common.command.body.TopicRouteData;
import com.clouditora.mq.common.command.header.GetRouteInfoRequestHeader;
import com.clouditora.mq.common.util.EnumUtil;
import com.clouditora.mq.network.CommandRequestProcessor;
import com.clouditora.mq.network.protocol.Command;
import com.clouditora.mq.network.protocol.ResponseCode;
import com.clouditora.top.nameserver.config.ConfigMapManager;
import com.clouditora.top.nameserver.NameserverConfig;
import com.clouditora.top.nameserver.route.RouteInfoManager;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

/**
 * @link org.apache.rocketmq.namesrv.processor.DefaultRequestProcessor
 */
@Slf4j
public class DefaultRequestProcessor implements CommandRequestProcessor {
    protected RouteInfoManager routeInfoManager;
    protected ConfigMapManager configManager;
    protected NameserverConfig config;

    @Override
    public Command process(ChannelHandlerContext context, Command request) throws Exception {
        RequestCode requestCode = EnumUtil.ofCode(RequestCode.class, request.getCode());
        switch (requestCode) {
            case GET_ROUTEINFO_BY_TOPIC:
                return this.getRouteInfoByTopic(context, request);
            default:
                log.error("unknown request code: {}", request);
                return null;
        }
    }

    /**
     * org.apache.rocketmq.namesrv.processor.DefaultRequestProcessor#getRouteInfoByTopic
     */
    private Command getRouteInfoByTopic(ChannelHandlerContext context, Command request) {
        GetRouteInfoRequestHeader requestHeader = request.extFieldsToHeader(GetRouteInfoRequestHeader.class);
        TopicRouteData topicRouteData = this.routeInfoManager.getTopicRouteData(requestHeader.getTopic());
        if (topicRouteData == null) {
            return Command.buildResponse(
                    ResponseCode.TOPIC_NOT_EXIST,
                    "topic %s not exits in nameserver".formatted(requestHeader.getTopic())
            );
        }
        if (this.config.isOrderMessageEnable()) {
            String orderTopicConf = this.configManager.get(ConfigMapManager.NAMESPACE_ORDER_TOPIC_CONFIG, requestHeader.getTopic());
            topicRouteData.setOrderTopicConf(orderTopicConf);
        }
        Command response = Command.buildResponse(ResponseCode.SUCCESS);
        response.setBody(topicRouteData.encode());
        return response;
    }
}
