package com.clouditora.mq.store.index;

import com.clouditora.mq.common.MessageConst;
import com.clouditora.mq.common.message.MessageEntity;
import com.clouditora.mq.store.MessageStoreConfig;
import com.clouditora.mq.store.enums.PutStatus;
import com.clouditora.mq.store.exception.PutException;
import com.clouditora.mq.store.log.MessageDispatcher;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class IndexFileDispatcher implements MessageDispatcher {
    private IndexFileQueue indexFileQueue;

    public IndexFileDispatcher(MessageStoreConfig config) {
        indexFileQueue = new IndexFileQueue(config);
    }

    @Override
    public void dispatch(MessageEntity message) throws Exception {
        IndexFile file = indexFileQueue.getOrCreate();
        if (file == null) {
            log.error("create file error: topic={}, bornHost={}", message.getTopic(), message.getBornHost());
            throw new PutException(PutStatus.CREATE_MAPPED_FILE_FAILED);
        }
        String key = buildKey(message);
        file.putKey(key, message.getCommitLogOffset(), message.getStoreTimestamp());
    }

    private String buildKey(MessageEntity message) {
        return message.getTopic() + "#" + message.getProperties().get(MessageConst.Property.MESSAGE_ID);
    }
}
