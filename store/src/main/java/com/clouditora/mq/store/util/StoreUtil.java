package com.clouditora.mq.store.util;

import com.clouditora.mq.common.MessageConst;
import org.apache.commons.lang3.StringUtils;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.CRC32;

public class StoreUtil {
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

    public static ByteBuffer socketAddress2ByteBuffer(InetSocketAddress socketAddress) {
        InetAddress address = socketAddress.getAddress();
        byte[] bytes = address.getAddress();
        ByteBuffer byteBuffer = ByteBuffer.allocate(bytes.length + 4);
        byteBuffer.put(bytes);
        byteBuffer.putInt(socketAddress.getPort());
        byteBuffer.flip();
        return byteBuffer;
    }

    final static char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();

    public static String bytes2string(byte[] src) {
        char[] hexChars = new char[src.length * 2];
        for (int j = 0; j < src.length; j++) {
            int v = src[j] & 0xFF;
            hexChars[j * 2] = HEX_ARRAY[v >>> 4];
            hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
        }
        return new String(hexChars);
    }

    private static final byte[] EMPTY_BYTES = new byte[0];

    public static byte[] properties2Bytes(Map<String, String> properties) {
        if (properties == null || properties.size() == 0) {
            return EMPTY_BYTES;
        }
        return properties.keySet().stream()
                .map(key -> key + MessageConst.Property.Separator.NAME_VALUE + properties.get(key))
                .reduce((a, b) -> a + MessageConst.Property.Separator.PROPERTY + b)
                .orElse(StringUtils.EMPTY)
                .getBytes(StandardCharsets.UTF_8);
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
