package com.clouditora.mq.common.constant;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Arrays;

@Getter
@AllArgsConstructor
public enum ClassCanonical {
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

    public static Class<?> of(String canonicalName) {
        return Arrays.stream(ClassCanonical.values())
                .filter(e -> e.getCanonicalName().equals(canonicalName))
                .findFirst()
                .map(ClassCanonical::getClazz)
                .orElse(null);
    }

    public static Object parseValue(String canonicalName, String value) {
        Class<?> type = of(canonicalName);
        if (type == Boolean.class) {
            return Boolean.valueOf(value);
        } else if (type == Integer.class) {
            return Integer.valueOf(value);
        } else if (type == Long.class) {
            return Long.valueOf(value);
        } else if (type == Double.class) {
            return Double.valueOf(value);
        } else if (type == String.class) {
            return value;
        }
        return null;
    }
}
