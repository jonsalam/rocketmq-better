package com.clouditora.mq.common.exception;

import lombok.Getter;

@Getter
public class BrokerException extends Exception {
    private final int code;
    private final String msg;
    private final String endpoint;

    public BrokerException(int code, String msg, String endpoint) {
        super("response code = %s: %s from %s".formatted(code, msg, endpoint));
        this.code = code;
        this.msg = msg;
        this.endpoint = endpoint;
    }
}
