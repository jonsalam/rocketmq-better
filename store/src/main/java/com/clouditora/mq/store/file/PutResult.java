package com.clouditora.mq.store.file;

import com.clouditora.mq.store.enums.PutStatus;
import lombok.Data;

import java.util.concurrent.CompletableFuture;

@Data
public class PutResult {
    private PutStatus status;
    private String messageId;
    private Long logicsOffset;

    public static CompletableFuture<PutResult> buildAsync(PutStatus status) {
        PutResult result = new PutResult();
        result.setStatus(status);
        return CompletableFuture.completedFuture(result);
    }
}
