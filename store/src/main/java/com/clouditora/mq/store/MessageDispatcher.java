package com.clouditora.mq.store;

import com.clouditora.mq.common.message.MessageEntity;

public interface MessageDispatcher {
    void dispatch(MessageEntity message) throws Exception;
}
