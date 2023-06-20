package com.clouditora.mq.store.log.flusher;

import com.clouditora.mq.common.message.MessageEntity;
import com.clouditora.mq.common.service.Lifecycle;
import com.clouditora.mq.store.file.PutResult;
import com.clouditora.mq.store.file.PutStatus;

import java.util.concurrent.CompletableFuture;

public interface CommitLogFlusher extends Lifecycle {
    /**
     * @link org.apache.rocketmq.store.CommitLog#submitFlushRequest
     */
    CompletableFuture<PutStatus> flush(CompletableFuture<PutResult> result, MessageEntity message);
}
