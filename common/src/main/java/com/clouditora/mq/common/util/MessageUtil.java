package com.clouditora.mq.common.util;

import com.clouditora.mq.common.MessageConst;
import org.apache.commons.lang3.StringUtils;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.CRC32;

public class MessageUtil {
    private static final byte[] EMPTY_BYTES = new byte[0];
    private static final String EMPTY_STRING = "";

    public static int crc32(byte[] array) {
        if (array != null) {
            return crc32(array, 0, array.length);
        }
        return 0;
    }

    public static int crc32(byte[] array, int offset, int length) {
        CRC32 crc32 = new CRC32();
        crc32.update(array, offset, length);
        return (int) (crc32.getValue() & 0x7FFFFFFF);
    }

    public static String properties2String(Map<String, String> properties) {
        if (properties == null || properties.size() == 0) {
            return EMPTY_STRING;
        }
        return properties.keySet().stream()
                .map(key -> key + MessageConst.Property.Separator.NAME_VALUE + properties.get(key))
                .reduce((a, b) -> a + MessageConst.Property.Separator.PROPERTY + b)
                .orElse(StringUtils.EMPTY);
    }

    public static byte[] properties2Bytes(Map<String, String> properties) {
        if (properties == null || properties.size() == 0) {
            return EMPTY_BYTES;
        }
        return properties2String(properties).getBytes(StandardCharsets.UTF_8);
    }

    public static Map<String, String> string2Properties(String properties) {
        Map<String, String> map = new HashMap<>();
        if (properties == null) {
            return map;
        }
        int len = properties.length();
        int index = 0;
        while (index < len) {
            int newIndex = properties.indexOf(MessageConst.Property.Separator.PROPERTY, index);
            if (newIndex < 0) {
                newIndex = len;
            }
            if (newIndex - index >= 3) {
                int kvSepIndex = properties.indexOf(MessageConst.Property.Separator.NAME_VALUE, index);
                if (kvSepIndex > index && kvSepIndex < newIndex - 1) {
                    String k = properties.substring(index, kvSepIndex);
                    String v = properties.substring(kvSepIndex + 1, newIndex);
                    map.put(k, v);
                }
            }
            index = newIndex + 1;
        }

        return map;
    }
}
