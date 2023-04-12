package com.clouditora.mq.common.util;

import com.clouditora.mq.common.constant.CodeEnum;

public class EnumUtil {
    public static <T extends Enum<?> & CodeEnum> T ofCode(int code, Class<T> clazz) {
        for (T t : clazz.getEnumConstants()) {
            if (t.getCode() == code) {
                return t;
            }
        }
        throw new RuntimeException(String.format("unknown enum: code=%s, class=%s", code, clazz));
    }
}
