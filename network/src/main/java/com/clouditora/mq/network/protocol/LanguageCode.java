package com.clouditora.mq.network.protocol;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Arrays;

/**
 * @link org.apache.rocketmq.remoting.protocol.LanguageCode
 */
@Getter
@AllArgsConstructor
public enum LanguageCode {
    JAVA((byte) 0),
    CPP((byte) 1),
    DOTNET((byte) 2),
    PYTHON((byte) 3),
    DELPHI((byte) 4),
    ERLANG((byte) 5),
    RUBY((byte) 6),
    OTHER((byte) 7),
    HTTP((byte) 8),
    GO((byte) 9),
    PHP((byte) 10),
    OMS((byte) 11),
    RUST((byte) 12);

    private final byte code;

    public static LanguageCode valueOf(byte code) {
        return Arrays.stream(LanguageCode.values())
                .filter(e -> e.getCode() == code)
                .findFirst()
                .orElse(null);
    }
}
