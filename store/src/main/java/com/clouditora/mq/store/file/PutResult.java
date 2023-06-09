package com.clouditora.mq.store.file;

import com.clouditora.mq.store.enums.PutMessageStatus;
import lombok.Data;

import java.util.concurrent.CompletableFuture;

@Data
public class PutResult {
    private PutMessageStatus status;
    private String messageId;
    private Long queueOffset;

    public static CompletableFuture<PutResult> buildAsync(PutMessageStatus status, String messageId, Long queueOffset) {
        PutResult result = new PutResult();
        result.setStatus(status);
        result.setMessageId(messageId);
        result.setQueueOffset(queueOffset);
        return CompletableFuture.completedFuture(result);
    }

    public static CompletableFuture<PutResult> buildAsync(PutMessageStatus status) {
        return buildAsync(status, null, null);
    }
}
