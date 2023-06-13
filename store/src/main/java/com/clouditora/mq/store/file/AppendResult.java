package com.clouditora.mq.store.file;

import lombok.Data;

import java.util.concurrent.CompletableFuture;

@Data
public class AppendResult {
    private AppendStatus status;
    private String messageId;
    private Long queueOffset;

    public static CompletableFuture<AppendResult> buildAsync(AppendStatus status, String messageId, Long queueOffset) {
        AppendResult result = new AppendResult();
        result.setStatus(status);
        result.setMessageId(messageId);
        result.setQueueOffset(queueOffset);
        return CompletableFuture.completedFuture(result);
    }

    public static CompletableFuture<AppendResult> buildAsync(AppendStatus status) {
        return buildAsync(status, null, null);
    }
}
