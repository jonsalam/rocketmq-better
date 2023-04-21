package com.clouditora.mq.common.constant;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * @link org.apache.rocketmq.remoting.protocol.SerializeType
 */
@Getter
@AllArgsConstructor
public enum SerializeType implements CodeEnum {
    JSON(0),
    BINARY(1);

    private final int code;
}
