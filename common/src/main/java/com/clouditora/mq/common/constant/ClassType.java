package com.clouditora.mq.common.constant;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Arrays;

@Getter
@AllArgsConstructor
public enum ClassType {
    BOOLEAN_1(Boolean.class, Boolean.class.getCanonicalName()),
    BOOLEAN_2(boolean.class, boolean.class.getCanonicalName()),
    INTEGER_1(Integer.class, Integer.class.getCanonicalName()),
    INTEGER_2(int.class, int.class.getCanonicalName()),
    LONG_1(Long.class, Long.class.getCanonicalName()),
    LONG_2(long.class, long.class.getCanonicalName()),
    DOUBLE_1(Double.class, Double.class.getCanonicalName()),
    DOUBLE_2(double.class, double.class.getCanonicalName()),
    STRING(String.class, String.class.getCanonicalName()),
    ;

    private final Class<?> clazz;
    private final String canonicalName;

    public Object parseValue(String value) {
        if (this == BOOLEAN_1 || this == BOOLEAN_2) {
            return Boolean.valueOf(value);
        } else if (this == INTEGER_1 || this == INTEGER_2) {
            return Integer.valueOf(value);
        } else if (this == LONG_1 || this == LONG_2) {
            return Long.valueOf(value);
        } else if (this == DOUBLE_1 || this == DOUBLE_2) {
            return Double.valueOf(value);
        } else if (this == STRING) {
            return value;
        }
        return null;
    }

    public static ClassType of(Class<?> clazz) {
        return Arrays.stream(ClassType.values())
                .filter(e -> e.getClazz() == clazz)
                .findFirst()
                .orElse(null);
    }

    public static ClassType of(String canonicalName) {
        return Arrays.stream(ClassType.values())
                .filter(e -> e.getCanonicalName().equals(canonicalName))
                .findFirst()
                .orElse(null);
    }

    @SuppressWarnings("unchecked")
    public static <T> T parseValue(Class<T> clazz, String value) {
        return (T) of(clazz).parseValue(value);
    }

    public static Object parseValue(String canonicalName, String value) {
        return of(canonicalName).parseValue(value);
    }
}
