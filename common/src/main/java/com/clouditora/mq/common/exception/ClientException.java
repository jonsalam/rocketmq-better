package com.clouditora.mq.common.exception;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@Data
public class ClientException extends Exception {
    private final int code;
    private final String msg;

    public ClientException(int code, String msg) {
        this.code = code;
        this.msg = msg;
    }
}
