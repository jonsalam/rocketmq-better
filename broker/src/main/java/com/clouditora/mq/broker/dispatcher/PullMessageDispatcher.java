package com.clouditora.mq.broker.dispatcher;

import com.clouditora.mq.broker.BrokerConfig;
import com.clouditora.mq.broker.client.TopicQueueConfigManager;
import com.clouditora.mq.broker.client.consumer.ConsumerManager;
import com.clouditora.mq.broker.client.consumer.ConsumerSubscribeManager;
import com.clouditora.mq.common.message.SysFlag;
import com.clouditora.mq.common.network.command.MessagePullCommand;
import com.clouditora.mq.common.topic.TopicQueueConfig;
import com.clouditora.mq.common.topic.TopicSubscription;
import com.clouditora.mq.network.command.AsyncCommandDispatcher;
import com.clouditora.mq.network.command.CommandDispatcher;
import com.clouditora.mq.network.protocol.Command;
import com.clouditora.mq.network.protocol.ResponseCode;
import com.clouditora.mq.network.util.NetworkUtil;
import com.clouditora.mq.store.StoreController;
import com.clouditora.mq.store.log.GetMessageResult;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

/**
 * @link org.apache.rocketmq.broker.processor.PullMessageProcessor
 */
@Slf4j
public class PullMessageDispatcher implements CommandDispatcher, AsyncCommandDispatcher {
    protected final BrokerConfig brokerConfig;
    protected final TopicQueueConfigManager topicQueueConfigManager;
    protected final ConsumerManager consumerManager;
    protected final StoreController storeController;

    public PullMessageDispatcher(BrokerConfig brokerConfig, TopicQueueConfigManager topicQueueConfigManager, ConsumerManager consumerManager, StoreController storeController) {
        this.brokerConfig = brokerConfig;
        this.topicQueueConfigManager = topicQueueConfigManager;
        this.consumerManager = consumerManager;
        this.storeController = storeController;
    }

    @Override
    public Command request(ChannelHandlerContext context, Command request) throws Exception {
        long beginTimeMills = System.currentTimeMillis();
        log.debug("receive pull message request: {}", request);
        MessagePullCommand.RequestHeader requestHeader = request.decodeHeader(MessagePullCommand.RequestHeader.class);

        MessagePullCommand.ResponseHeader responseHeader = new MessagePullCommand.ResponseHeader();
        Command response = Command.buildResponse(ResponseCode.SUCCESS);
        response.setHeader(responseHeader);
        response.setOpaque(request.getOpaque());

//        boolean hasSuspendFlag = PullSysFlag.hasSuspendFlag(requestHeader.getSysFlag());
//        boolean hasCommitOffsetFlag = PullSysFlag.hasCommitOffsetFlag(requestHeader.getSysFlag());
        boolean hasSubscriptionFlag = SysFlag.hasSubscriptionFlag(requestHeader.getSysFlag());
//        long suspendTimeoutMillisLong = hasSuspendFlag ? requestHeader.getSuspendTimeoutMillis() : 0;

        TopicQueueConfig topicConfig = this.topicQueueConfigManager.get(requestHeader.getTopic());
        if (topicConfig == null) {
            log.warn("topic not exist:{}, consumer={}", requestHeader.getTopic(), NetworkUtil.toEndpoint(context.channel()));
            response.setCode(ResponseCode.TOPIC_NOT_EXIST);
            response.setRemark(String.format("topic %s not exist", requestHeader.getTopic()));
            return response;
        }

        if (requestHeader.getQueueId() < 0 || requestHeader.getQueueId() >= topicConfig.getReadQueueNum()) {
            log.warn("queueId is illegal: {}, topic={}, readQueueNum={}, consumer={}", requestHeader.getQueueId(), requestHeader.getTopic(), topicConfig.getReadQueueNum(), NetworkUtil.toEndpoint(context.channel()));
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("queueId is illegal: %s, readQueueNum=%s".formatted(requestHeader.getQueueId(), topicConfig.getReadQueueNum()));
            return response;
        }

        TopicSubscription subscription = null;
        if (hasSubscriptionFlag) {
            try {
                subscription = TopicSubscription.build(requestHeader.getTopic(), requestHeader.getExpression(), requestHeader.getExpressionType());
            } catch (Exception e) {
                log.warn("build subscription failed: group={}, expression={}, consumer={}", requestHeader.getGroup(), requestHeader.getExpression(), NetworkUtil.toEndpoint(context.channel()));
                response.setCode(ResponseCode.SUBSCRIPTION_PARSE_FAILED);
                response.setRemark("parse the consumer's subscription failed");
                return response;
            }
        } else {
            ConsumerSubscribeManager subscribeManager = this.consumerManager.getConsumerSubscription(requestHeader.getGroup());
            if (subscribeManager == null) {
                log.warn("the consumer's group info not exist, group: {}", requestHeader.getGroup());
                response.setCode(ResponseCode.SUBSCRIPTION_NOT_EXIST);
                response.setRemark("the consumer's group info not exist");
                return response;
            }

            subscription = subscribeManager.get(requestHeader.getTopic());
            if (subscription == null) {
                log.warn("the consumer's subscription not exist, group: {}, topic:{}", requestHeader.getGroup(), requestHeader.getTopic());
                response.setCode(ResponseCode.SUBSCRIPTION_NOT_EXIST);
                response.setRemark("the consumer's subscription not exist");
                return response;
            }
        }

        GetMessageResult result = this.storeController.get(
                requestHeader.getGroup(),
                requestHeader.getTopic(),
                requestHeader.getQueueId(),
                requestHeader.getPullOffset(),
                requestHeader.getPullNum(),
                subscription
        );
//        if (result == null) {
//            response.setCode(ResponseCode.SYSTEM_ERROR);
//            response.setRemark("store getMessage return null");
//        } else {
//            responseHeader.setNextBeginOffset(result.getNextBeginOffset());
//            responseHeader.setMinOffset(result.getMinOffset());
//            responseHeader.setMaxOffset(result.getMaxOffset());
//            response.setRemark(result.getStatus().name());
//
//            switch (result.getStatus()) {
//                case OFFSET_OVERFLOW -> {
//                    response.setCode(ResponseCode.PULL_OFFSET_MOVED);
//                    // XXX: warn and notify me
//                    log.info("the request offset: {} over flow badly, broker max offset: {}, consumer: {}", requestHeader.getPullOffset(), result.getMaxOffset(), channel.remoteAddress());
//                }
//                case OFFSET_TOO_SMALL -> {
//                    response.setCode(ResponseCode.PULL_OFFSET_MOVED);
//                    log.info("the request offset too small. group={}, topic={}, requestOffset={}, brokerMinOffset={}, clientIp={}",
//                            requestHeader.getConsumerGroup(), requestHeader.getTopic(), requestHeader.getPullOffset(),
//                            result.getMinOffset(), channel.remoteAddress());
//                }
//                default -> {
//                }
//            }
//
//            switch (result.getStatus()) {
//                case FOUND -> response = onSuccess(context, requestHeader, response, result);
//                case MESSAGE_WAS_REMOVING, NO_MATCHED_MESSAGE -> response.setCode(ResponseCode.PULL_RETRY_IMMEDIATELY);
//                case NO_MATCHED_LOGIC_QUEUE, NO_MESSAGE_IN_QUEUE -> {
//                    if (requestHeader.getPullOffset() == 0) {
//                        response = onNotFound(request, requestHeader, response, hasSuspendFlag, suspendTimeoutMillisLong, subscription, messageFilter);
//                    } else {
//                        // XXX: warn and notify me
//                        log.info("the broker store no queue data, fix the request offset {} to {}, Topic: {} QueueId: {} Consumer Group: {}",
//                                requestHeader.getPullOffset(),
//                                result.getNextBeginOffset(),
//                                requestHeader.getTopic(),
//                                requestHeader.getQueueId(),
//                                requestHeader.getGroup()
//                        );
//                        response = onOffsetMoved(requestHeader, responseHeader, response, result);
//                    }
//                }
//                case OFFSET_FOUND_NULL, OFFSET_OVER ->
//                        response = onNotFound(request, requestHeader, response, hasSuspendFlag, suspendTimeoutMillisLong, subscription, messageFilter);
//                case OFFSET_OVERFLOW -> {
//                    // XXX: warn and notify me
//                    log.info("the request offset: {} over flow badly, broker max offset: {}, consumer: {}", requestHeader.getPullOffset(), result.getMaxOffset(), channel.remoteAddress());
//                    response = onOffsetMoved(requestHeader, responseHeader, response, result);
//                }
//                case OFFSET_TOO_SMALL -> {
//                    log.info("the request offset too small. group={}, topic={}, requestOffset={}, brokerMinOffset={}, clientIp={}",
//                            requestHeader.getGroup(), requestHeader.getTopic(), requestHeader.getPullOffset(),
//                            result.getMinOffset(), channel.remoteAddress());
//                    response = onOffsetMoved(requestHeader, responseHeader, response, result);
//                }
//                default -> {
//                }
//            }
//        }
//
//        boolean storeOffsetEnable = brokerAllowSuspend;
//        storeOffsetEnable = storeOffsetEnable && hasCommitOffsetFlag;
//        storeOffsetEnable = storeOffsetEnable && this.brokerController.getMessageStoreConfig().getBrokerRole() != BrokerRole.SLAVE;
//        if (storeOffsetEnable) {
//            this.brokerController.getConsumerOffsetManager().commitOffset(RemotingHelper.parseChannelRemoteAddr(channel), requestHeader.getConsumerGroup(), requestHeader.getTopic(), requestHeader.getQueueId(), requestHeader.getCommitOffset());
//        }
        return response;
    }

    private Command onSuccess(ChannelHandlerContext context, MessagePullCommand.RequestHeader requestHeader, Command response, GetMessageResult result) {
//        if (this.brokerController.getBrokerConfig().isTransferMsgByHeap()) {
//            byte[] r = this.readGetMessageResult(result, requestHeader.getConsumerGroup(), requestHeader.getTopic(), requestHeader.getQueueId());
//            response.setBody(r);
//        } else {
//            try {
//                FileRegion fileRegion = new ManyMessageTransfer(response.encodeHeader(result.getBufferTotalSize()), result);
//                context.channel().writeAndFlush(fileRegion).addListener(new ChannelFutureListener() {
//                    @Override
//                    public void operationComplete(ChannelFuture future) throws Exception {
//                        result.release();
//                        if (!future.isSuccess()) {
//                            log.error("transfer many message by pagecache failed, {}", channel.remoteAddress(), future.cause());
//                        }
//                    }
//                });
//            } catch (Throwable e) {
//                log.error("transfer many message by pagecache exception", e);
//                result.release();
//            }
//
//            response = null;
//        }
        return response;
    }

    private Command onNotFound(Command request, MessagePullCommand.RequestHeader requestHeader, Command response, boolean hasSuspendFlag, long suspendTimeoutMillisLong, TopicSubscription subscription) {
//        if (hasSuspendFlag) {
//            long pollingTimeMills = suspendTimeoutMillisLong;
//            if (!this.brokerController.getBrokerConfig().isLongPollingEnable()) {
//                pollingTimeMills = this.brokerController.getBrokerConfig().getShortPollingTimeMills();
//            }
//
//            String topic = requestHeader.getTopic();
//            long offset = requestHeader.getPullOffset();
//            int queueId = requestHeader.getQueueId();
//            PullRequest pullRequest = new PullRequest(request, channel, pollingTimeMills, this.brokerController.getMessageStore().now(), offset, subscription, messageFilter);
//            this.brokerController.getPullRequestHoldService().suspendPullRequest(topic, queueId, pullRequest);
//        }
        return response;
    }

    private Command onOffsetMoved(MessagePullCommand.RequestHeader requestHeader, MessagePullCommand.ResponseHeader responseHeader, Command response, GetMessageResult result) {
//        if (this.brokerController.getMessageStoreConfig().getBrokerRole() != BrokerRole.SLAVE
//                || this.brokerController.getMessageStoreConfig().isOffsetCheckInSlave()) {
//            TopicQueue mq = new TopicQueue();
//            mq.setTopic(requestHeader.getTopic());
//            mq.setBrokerName(this.brokerConfig.getBrokerName());
//            mq.setQueueId(requestHeader.getQueueId());
//
//            OffsetMovedEvent event = new OffsetMovedEvent();
//            event.setConsumerGroup(requestHeader.getConsumerGroup());
//            event.setMessageQueue(mq);
//            event.setOffsetRequest(requestHeader.getPullOffset());
//            event.setOffsetNew(result.getNextBeginOffset());
//            this.generateOffsetMovedEvent(event);
//            log.warn(
//                    "PULL_OFFSET_MOVED:correction offset. topic={}, groupId={}, requestOffset={}, newOffset={}, suggestBrokerId={}",
//                    requestHeader.getTopic(), requestHeader.getConsumerGroup(), event.getOffsetRequest(), event.getOffsetNew(),
//                    responseHeader.getSuggestWhichBrokerId());
//        } else {
//            responseHeader.setSuggestWhichBrokerId(subscriptionGroupConfig.getBrokerId());
//            response.setCode(ResponseCode.PULL_RETRY_IMMEDIATELY);
//            log.warn("PULL_OFFSET_MOVED:none correction. topic={}, groupId={}, requestOffset={}, suggestBrokerId={}",
//                    requestHeader.getTopic(), requestHeader.getConsumerGroup(), requestHeader.getPullOffset(),
//                    responseHeader.getSuggestWhichBrokerId());
//        }
        return response;
    }
}
