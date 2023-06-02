package com.clouditora.mq.broker.dispatcher;

import com.clouditora.mq.broker.client.ConsumerLockManager;
import com.clouditora.mq.common.network.RequestCode;
import com.clouditora.mq.common.network.command.LockQueueCommand;
import com.clouditora.mq.common.network.command.UnlockQueueCommand;
import com.clouditora.mq.common.topic.TopicQueue;
import com.clouditora.mq.common.util.EnumUtil;
import com.clouditora.mq.network.command.CommandDispatcher;
import com.clouditora.mq.network.protocol.Command;
import com.clouditora.mq.network.protocol.ResponseCode;
import com.clouditora.mq.network.util.NetworkUtil;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import java.util.Set;

/**
 * @link org.apache.rocketmq.broker.processor.AdminBrokerProcessor
 */
@Slf4j
public class AdminBrokerDispatcher implements CommandDispatcher {
    private final ConsumerLockManager consumerLockManager;

    public AdminBrokerDispatcher(ConsumerLockManager consumerLockManager) {
        this.consumerLockManager = consumerLockManager;
    }

    @Override
    public Command request(ChannelHandlerContext context, Command request) throws Exception {
        RequestCode requestCode = EnumUtil.ofCode(request.getCode(), RequestCode.class);
        switch (requestCode) {
            case LOCK_BATCH_MQ -> {
                return lockQueue(context, request);
            }
            case UNLOCK_BATCH_MQ -> {
                return unlockQueue(context, request);
            }
            default -> {
                log.error("{} request code [{}] is not supported", NetworkUtil.toEndpoint(context.channel()), request.getCode());
                return null;
            }
        }
    }

    /**
     * @link org.apache.rocketmq.broker.processor.AdminBrokerProcessor#lockBatchMQ
     */
    private Command lockQueue(ChannelHandlerContext context, Command request) {
        LockQueueCommand.RequestBody requestBody = request.decodeBody(LockQueueCommand.RequestBody.class);
        Set<TopicQueue> lockedQueues = consumerLockManager.lockQueue(requestBody.getGroup(), requestBody.getQueues(), requestBody.getClientId());

        LockQueueCommand.ResponseBody responseBody = new LockQueueCommand.ResponseBody();
        responseBody.setQueues(lockedQueues);
        Command response = Command.buildResponse(ResponseCode.SUCCESS);
        response.setBody(responseBody);
        return response;
    }

    /**
     * @link org.apache.rocketmq.broker.processor.AdminBrokerProcessor#unlockBatchMQ
     */
    private Command unlockQueue(ChannelHandlerContext context, Command request) {
        UnlockQueueCommand.RequestBody requestBody = request.decodeBody(UnlockQueueCommand.RequestBody.class);
        consumerLockManager.unlockQueue(requestBody.getGroup(), requestBody.getQueues(), requestBody.getClientId());
        return Command.buildResponse(ResponseCode.SUCCESS);
    }
}
