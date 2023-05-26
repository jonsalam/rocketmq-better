package com.clouditora.mq.broker.dispatcher;

import com.clouditora.mq.broker.BrokerConfig;
import com.clouditora.mq.broker.BrokerController;
import com.clouditora.mq.common.MessageConst;
import com.clouditora.mq.common.network.RequestCode;
import com.clouditora.mq.common.network.command.MessageSendCommand;
import com.clouditora.mq.common.util.EnumUtil;
import com.clouditora.mq.common.util.MessageUtil;
import com.clouditora.mq.network.command.AsyncCommandDispatcher;
import com.clouditora.mq.network.command.CommandCallback;
import com.clouditora.mq.network.command.CommandDispatcher;
import com.clouditora.mq.network.protocol.Command;
import com.clouditora.mq.network.util.NetworkUtil;
import com.clouditora.mq.store.MessageEntity;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;
import java.util.Optional;

/**
 * @link org.apache.rocketmq.broker.processor.SendMessageProcessor
 */
@Slf4j
public class SendMessageDispatcher implements CommandDispatcher, AsyncCommandDispatcher {
    protected final BrokerController brokerController;
    protected final BrokerConfig brokerConfig;
    protected final InetSocketAddress storeHost;

    public SendMessageDispatcher(BrokerController brokerController, BrokerConfig brokerConfig) {
        this.brokerController = brokerController;
        this.brokerConfig = brokerConfig;
        this.storeHost = new InetSocketAddress(brokerConfig.getBrokerIp(), brokerConfig.getBrokerPort());
    }

    @Override
    public Command request(ChannelHandlerContext context, Command request) throws Exception {
        RequestCode requestCode = EnumUtil.ofCode(request.getCode(), RequestCode.class);
        switch (requestCode) {
            case SEND_MESSAGE -> {
                MessageSendCommand.RequestHeader requestHeader = request.decodeHeader(MessageSendCommand.RequestHeader.class);
                return receiveMessage(context, request, requestHeader);
            }
            case SEND_MESSAGE_V2 -> {
                MessageSendCommand.RequestHeaderV2 requestHeader = request.decodeHeader(MessageSendCommand.RequestHeaderV2.class);
                return receiveMessage(context, request, requestHeader);
            }
            default -> {
                log.error("{} request code [{}] is not supported", NetworkUtil.toEndpoint(context.channel()), request.getCode());
                return null;
            }
        }
    }

    @Override
    public void asyncRequest(ChannelHandlerContext context, Command request, CommandCallback callback) throws Exception {
        AsyncCommandDispatcher.super.asyncRequest(context, request, callback);
    }

    /**
     * @link org.apache.rocketmq.broker.processor.SendMessageProcessor#asyncSendMessage
     */
    private Command receiveMessage(ChannelHandlerContext context, Command request, MessageSendCommand.RequestHeader requestHeader) {
        Command response = Command.buildResponse();
        response.setOpaque(request.getOpaque());

        MessageEntity message = new MessageEntity();
        message.setTopic(requestHeader.getTopic());
        message.setQueueId(requestHeader.getQueueId());
        message.setBody(request.getBody());
        message.setFlag(requestHeader.getFlag());
        message.setProperties(MessageUtil.string2Properties(requestHeader.getProperties()));
        message.setBornTimestamp(requestHeader.getBornTimestamp());
        message.setBornHost((InetSocketAddress) context.channel().remoteAddress());
        message.setStoreHost(this.storeHost);
        message.setReConsumeTimes(Optional.ofNullable(requestHeader.getReConsumeTimes()).orElse(0));
        message.putProperty(MessageConst.Property.CLUSTER, brokerConfig.getBrokerClusterName());
        this.brokerController.putMessage(message);
        return response;
    }

}
