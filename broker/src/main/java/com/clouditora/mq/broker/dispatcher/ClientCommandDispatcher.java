package com.clouditora.mq.broker.dispatcher;

import com.clouditora.mq.broker.BrokerController;
import com.clouditora.mq.broker.client.ClientChannel;
import com.clouditora.mq.common.network.RequestCode;
import com.clouditora.mq.common.network.command.ClientRegisterCommand;
import com.clouditora.mq.common.network.command.ClientUnregisterCommand;
import com.clouditora.mq.common.util.EnumUtil;
import com.clouditora.mq.network.command.CommandDispatcher;
import com.clouditora.mq.network.protocol.Command;
import com.clouditora.mq.network.protocol.ResponseCode;
import com.clouditora.mq.network.util.NetworkUtil;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ClientCommandDispatcher implements CommandDispatcher {
    private final BrokerController brokerController;

    public ClientCommandDispatcher(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    @Override
    public Command request(ChannelHandlerContext context, Command request) throws Exception {
        RequestCode requestCode = EnumUtil.ofCode(request.getCode(), RequestCode.class);
        switch (requestCode) {
            case REGISTER_CLIENT -> {
                return registerClient(context, request);
            }
            case UNREGISTER_CLIENT -> {
                return unregisterClient(context, request);
            }
            default -> {
                log.error("{} request code [{}] is not supported", NetworkUtil.toEndpoint(context.channel()), request.getCode());
                return null;
            }
        }
    }

    private Command registerClient(ChannelHandlerContext context, Command request) {
        ClientRegisterCommand.RequestBody requestBody = request.decodeBody(ClientRegisterCommand.RequestBody.class);
        ClientChannel clientChannel = new ClientChannel();
        clientChannel.setChannel(context.channel());
        clientChannel.setClientId(requestBody.getClientId());
        clientChannel.setLanguage(request.getLanguage());
        clientChannel.setUpdateTime(System.currentTimeMillis());
        brokerController.registerClient(clientChannel, requestBody.getProducers(), requestBody.getConsumers());
        return Command.buildResponse(ResponseCode.SUCCESS);
    }

    private Command unregisterClient(ChannelHandlerContext context, Command request) {
        ClientUnregisterCommand.RequestHeader requestHeader = request.decodeHeader(ClientUnregisterCommand.RequestHeader.class);
        ClientChannel clientChannel = new ClientChannel();
        clientChannel.setChannel(context.channel());
        clientChannel.setClientId(requestHeader.getClientId());
        clientChannel.setLanguage(request.getLanguage());
        clientChannel.setUpdateTime(System.currentTimeMillis());

        brokerController.unregisterClient(clientChannel, requestHeader.getProducerGroup(), requestHeader.getConsumerGroup());
        return Command.buildResponse(ResponseCode.SUCCESS);
    }

}
