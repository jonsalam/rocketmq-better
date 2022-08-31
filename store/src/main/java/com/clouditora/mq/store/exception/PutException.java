package com.clouditora.mq.store.exception;

import com.clouditora.mq.store.enums.PutStatus;
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
