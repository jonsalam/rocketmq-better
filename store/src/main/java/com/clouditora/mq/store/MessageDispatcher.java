package com.clouditora.mq.store;

public interface MessageDispatcher {
    void dispatch(MessageEntity message) throws Exception;
}
