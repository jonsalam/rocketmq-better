package com.clouditora.mq.common.util;

public class NumberUtil {
    public static boolean isNumber(String value) {
        try {
            Long.parseLong(value);
            return true;
        } catch (Exception ignored) {
            return false;
        }
    }
}
