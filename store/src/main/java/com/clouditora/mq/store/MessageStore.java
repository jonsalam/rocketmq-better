package com.clouditora.mq.store;

import com.clouditora.mq.store.exception.PutException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MessageStore {
    private final MessageStoreConfig storeConfig;
    private final CommitLog commitLog;

    public MessageStore(MessageStoreConfig storeConfig) {
        this.storeConfig = storeConfig;
        this.commitLog = new CommitLog(storeConfig);
    }

    /**
     * org.apache.rocketmq.store.DefaultMessageStore#asyncPutMessage
     */
    public void putMessage(MessageEntity message) throws PutException {
        commitLog.putMessage(message);
    }
}
