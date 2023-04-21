package com.clouditora.mq.common.util;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

public class TimeUtil {
    private static String format(String format, long timestamp) {
        LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault());
        return String.format(
                format,
                localDateTime.getYear(),
                localDateTime.getMonthValue(),
                localDateTime.getDayOfMonth(),
                localDateTime.getHour(),
                localDateTime.getMinute(),
                localDateTime.getSecond()
        );
    }

    public static String timeMillisToHumanString2(long timestamp) {
        return format("%04d-%02d-%02d %02d:%02d:%02d,%03d", timestamp);
    }

    public static String timeMillisToHumanString3(long timestamp) {
        return format("%04d%02d%02d%02d%02d%02d", timestamp);
    }
}
