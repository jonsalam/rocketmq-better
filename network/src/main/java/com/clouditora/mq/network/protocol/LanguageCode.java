package com.clouditora.mq.network.protocol;

import com.clouditora.mq.common.constant.CodeEnum;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * @link org.apache.rocketmq.remoting.protocol.LanguageCode
 */
@Getter
@AllArgsConstructor
public enum LanguageCode implements CodeEnum {
    JAVA(0),
    CPP(1),
    DOTNET(2),
    PYTHON(3),
    DELPHI(4),
    ERLANG(5),
    RUBY(6),
    OTHER(7),
    HTTP(8),
    GO(9),
    PHP(10),
    OMS(11),
    RUST(12);

    private final int code;
}
