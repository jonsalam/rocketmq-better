package com.clouditora.mq.store.index;

import com.clouditora.mq.common.MessageConst;
import com.clouditora.mq.store.MessageDispatcher;
import com.clouditora.mq.store.MessageEntity;
import com.clouditora.mq.store.MessageStoreConfig;
import com.clouditora.mq.store.enums.PutStatus;
import com.clouditora.mq.store.exception.PutException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class IndexFileDispatcher implements MessageDispatcher {
    private IndexFileQueue indexFileQueue;

    public IndexFileDispatcher(MessageStoreConfig config) {
        indexFileQueue = new IndexFileQueue(config);
    }

    @Override
    public void dispatch(MessageEntity message) throws Exception {
        IndexFile file = indexFileQueue.getCurrentWritingFile();
        if (file == null) {
            log.error("create file error: topic={}, bornHost={}", message.getTopic(), message.getBornHost());
            throw new PutException(PutStatus.CREATE_MAPPED_FILE_FAILED);
        }
        String key = buildKey(message);
        file.putKey(key, message.getLogOffset(), message.getStoreTimestamp());
    }

    private String buildKey(MessageEntity message) {
        return message.getTopic() + "#" + message.getProperties().get(MessageConst.Property.MESSAGE_ID);
    }
}
