package com.clouditora.mq.common.constant;

import com.clouditora.mq.common.constant.CodeEnum;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Arrays;

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
