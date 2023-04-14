package com.clouditora.mq.network.protocol;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Arrays;

/**
 * @link org.apache.rocketmq.remoting.protocol.SerializeType
 */
@Getter
@AllArgsConstructor
public enum SerializeType {
    JSON((byte) 0),
    BINARY((byte) 1);

    private final byte code;

    public static SerializeType valueOf(byte code) {
        return Arrays.stream(SerializeType.values())
                .filter(e -> e.getCode() == code)
                .findFirst()
                .orElse(null);
    }
}
