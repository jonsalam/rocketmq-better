package com.clouditora.mq.common.exception;

import lombok.Getter;

@Getter
public class ClientException extends Exception {
    private final int code;
    private final String msg;

    public ClientException(int code, String msg, Throwable cause) {
        super(msg, cause);
        this.code = code;
        this.msg = msg;
    }

    public ClientException(int code, String msg) {
        this(code, msg, null);
    }

    public ClientException(String msg, Throwable cause) {
        this(-1, msg, cause);
    }
}
