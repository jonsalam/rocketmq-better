package com.clouditora.mq.store;

import com.clouditora.mq.store.exception.PutException;
import com.clouditora.mq.store.index.ConsumeFileMap;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MessageStore {
    private final MessageStoreConfig storeConfig;
    private final CommitLog commitLog;
    private final ConsumeFileMap consumeFileMap;
    private final Thread dispatcher;

    public MessageStore(MessageStoreConfig storeConfig) {
        this.storeConfig = storeConfig;
        this.commitLog = new CommitLog(storeConfig);
        this.consumeFileMap = new ConsumeFileMap(storeConfig);
        this.dispatcher = new Thread(new CommitLogDispatcher(this.storeConfig, this.commitLog, this.consumeFileMap));
    }

    public void start() {
        dispatcher.start();
    }

    /**
     * org.apache.rocketmq.store.DefaultMessageStore#asyncPutMessage
     */
    public void putMessage(MessageEntity message) throws PutException {
        commitLog.putMessage(message);
    }
}
