package com.clouditora.mq.store.exception;

import com.clouditora.mq.store.file.PutStatus;
import lombok.Getter;

public class PutException extends Exception {
    @Getter
    private PutStatus status;

    public PutException() {
    }

    public PutException(String desc) {
        super(desc);
    }

    public PutException(PutStatus status) {
        this.status = status;
    }
}
