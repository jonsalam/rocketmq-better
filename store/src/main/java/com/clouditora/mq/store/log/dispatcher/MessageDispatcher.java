package com.clouditora.mq.store.log.dispatcher;

import com.clouditora.mq.common.message.MessageEntity;

/**
 * @link org.apache.rocketmq.store.CommitLogDispatcher
 */
public interface MessageDispatcher {
    void dispatch(MessageEntity message) throws Exception;
}
