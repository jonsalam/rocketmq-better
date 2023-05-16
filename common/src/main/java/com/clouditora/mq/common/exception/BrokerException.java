package com.clouditora.mq.common.exception;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@Data
public class BrokerException extends Exception {
    private final int code;
    private final String msg;
    private final String endpoint;

    public BrokerException(int code, String msg, String endpoint) {
        super(msg);
        this.code = code;
        this.msg = msg;
        this.endpoint = endpoint;
    }
}
