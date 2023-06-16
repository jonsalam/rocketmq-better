package com.clouditora.mq.store.file;

import lombok.Data;

import java.util.concurrent.CompletableFuture;

@Data
public class PutResult {
    private PutStatus status;
    private String messageId;
    private Long queueOffset;

    public static CompletableFuture<PutResult> buildAsync(PutStatus status, String messageId, Long queueOffset) {
        PutResult result = new PutResult();
        result.setStatus(status);
        result.setMessageId(messageId);
        result.setQueueOffset(queueOffset);
        return CompletableFuture.completedFuture(result);
    }

    public static CompletableFuture<PutResult> buildAsync(PutStatus status) {
        return buildAsync(status, null, null);
    }
}
