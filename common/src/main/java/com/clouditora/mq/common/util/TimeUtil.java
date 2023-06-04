package com.clouditora.mq.common.util;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class TimeUtil {

    /**
     * like 20131223171201
     *
     * @link org.apache.rocketmq.common.UtilAll#timeMillisToHumanString3
     */
    public static String timestampToHumanString3(long timestamp) {
        LocalDateTime localDateTime = Instant.ofEpochMilli(timestamp).atZone(ZoneId.systemDefault()).toLocalDateTime();
        return localDateTime.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
    }

    public static long humanString3ToTimestamp(String string) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
        return LocalDateTime.parse(string, formatter).atZone(ZoneId.systemDefault()).toEpochSecond();
    }
}
