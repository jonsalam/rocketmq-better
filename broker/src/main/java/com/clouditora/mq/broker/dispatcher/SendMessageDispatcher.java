package com.clouditora.mq.broker.dispatcher;

import com.clouditora.mq.broker.BrokerConfig;
import com.clouditora.mq.common.MessageConst;
import com.clouditora.mq.common.message.MessageEntity;
import com.clouditora.mq.common.network.RequestCode;
import com.clouditora.mq.common.network.command.MessageSendCommand;
import com.clouditora.mq.common.util.EnumUtil;
import com.clouditora.mq.common.util.MessageUtil;
import com.clouditora.mq.network.command.AsyncCommandDispatcher;
import com.clouditora.mq.network.command.CommandCallback;
import com.clouditora.mq.network.command.CommandDispatcher;
import com.clouditora.mq.network.protocol.Command;
import com.clouditora.mq.network.protocol.ResponseCode;
import com.clouditora.mq.network.util.NetworkUtil;
import com.clouditora.mq.store.MessageStore;
import com.clouditora.mq.store.file.PutResult;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * @link org.apache.rocketmq.broker.processor.SendMessageProcessor
 */
@Slf4j
public class SendMessageDispatcher implements CommandDispatcher, AsyncCommandDispatcher {
    protected final BrokerConfig brokerConfig;
    protected final MessageStore messageStore;
    protected final InetSocketAddress storeHost;

    public SendMessageDispatcher(BrokerConfig brokerConfig, MessageStore messageStore) {
        this.brokerConfig = brokerConfig;
        this.messageStore = messageStore;
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
        MessageSendCommand.ResponseHeader responseHeader = new MessageSendCommand.ResponseHeader();
        Command response = Command.buildResponse(ResponseCode.SUCCESS);
        response.setHeader(responseHeader);
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
        CompletableFuture<PutResult> result = this.messageStore.asyncPut(message);
        result.thenApply(e -> {
            if (e == null) {
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("store message return null");
                return response;
            }
            switch (e.getStatus()) {
                case SUCCESS -> response.setCode(ResponseCode.SUCCESS);
                case CREATE_MAPPED_FILE_FAILED -> {
                    response.setCode(ResponseCode.SYSTEM_ERROR);
                    response.setRemark("create mapped file failed, server is busy or broken.");
                }
                case MESSAGE_ILLEGAL -> {
                    response.setCode(ResponseCode.MESSAGE_ILLEGAL);
                    response.setRemark("the message is illegal, maybe msg body or properties length not matched.");
                }
                case UNKNOWN_ERROR -> {
                    response.setCode(ResponseCode.SYSTEM_ERROR);
                    response.setRemark("UNKNOWN_ERROR");
                }
                default -> {
                    response.setCode(ResponseCode.SYSTEM_ERROR);
                    response.setRemark("UNKNOWN_ERROR DEFAULT");
                }
            }
            responseHeader.setMessageId(e.getMessageId());
            responseHeader.setQueueId(requestHeader.getQueueId());
            responseHeader.setQueueOffset(e.getQueueOffset());
            return response;
        });
        return response;
    }

}
