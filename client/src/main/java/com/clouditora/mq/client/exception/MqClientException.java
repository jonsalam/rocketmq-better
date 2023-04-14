package com.clouditora.mq.client.exception;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * @link org.apache.rocketmq.client.exception.MQClientException
 */
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@Data
public class MqClientException extends Exception {
    private int code;

    public MqClientException(String message) {
        super(message);
        this.code= -1;
    }

    public MqClientException(int code, String message) {
        super(message);
        this.code = code;
    }

    public MqClientException(String message, Exception e) {
        super(message, e);
        this.code = -1;
    }
}
