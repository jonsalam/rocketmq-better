package com.clouditora.mq.broker.processor;

import com.clouditora.mq.broker.BrokerController;
import com.clouditora.mq.broker.client.ClientChannel;
import com.clouditora.mq.common.command.RequestCode;
import com.clouditora.mq.common.command.protocol.ClientHeartBeatCommand;
import com.clouditora.mq.common.util.EnumUtil;
import com.clouditora.mq.network.CommandRequestProcessor;
import com.clouditora.mq.network.protocol.Command;
import com.clouditora.mq.network.util.CoordinatorUtil;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DefaultRequestProcessor implements CommandRequestProcessor {
    private final BrokerController brokerController;

    public DefaultRequestProcessor(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    @Override
    public Command process(ChannelHandlerContext context, Command request) throws Exception {
        RequestCode requestCode = EnumUtil.ofCode(request.getCode(), RequestCode.class);
        switch (requestCode) {
            case HEART_BEAT -> {
                return heartBeat(context, request);
            }
            case UNREGISTER_CLIENT -> {
                return unregisterClient(context, request);
            }
            default -> {
                log.error("{} request code [{}] is not supported", CoordinatorUtil.toEndpoint(context.channel()), request.getCode());
                return null;
            }
        }
    }

    private Command heartBeat(ChannelHandlerContext context, Command request) {
        ClientHeartBeatCommand.RequestBody requestBody = request.decodeBody(ClientHeartBeatCommand.RequestBody.class);
        ClientChannel clientChannel = new ClientChannel();
        clientChannel.setChannel(context.channel());
        clientChannel.setClientId(requestBody.getClientId());
        clientChannel.setLanguage(request.getLanguage());
        clientChannel.setUpdateTime(System.currentTimeMillis());
//        brokerController.heartBeat(clientChannel, requestBody.getProducers(), requestBody.getConsumers());
        return Command.buildResponse();
    }

    private Command unregisterClient(ChannelHandlerContext context, Command request) {

        return null;
    }
}
