package com.clouditora.mq.store.exception;

import com.clouditora.mq.store.file.AppendStatus;
import lombok.Getter;

public class PutException extends Exception {
    @Getter
    private AppendStatus status;

    public PutException() {
    }

    public PutException(String desc) {
        super(desc);
    }

    public PutException(AppendStatus status) {
        this.status = status;
    }
}
