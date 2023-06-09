package com.clouditora.mq.store.exception;

import com.clouditora.mq.store.enums.PutMessageStatus;
import lombok.Getter;

public class PutException extends Exception {
    @Getter
    private PutMessageStatus status;

    public PutException() {
    }

    public PutException(String desc) {
        super(desc);
    }

    public PutException(PutMessageStatus status) {
        this.status = status;
    }
}
