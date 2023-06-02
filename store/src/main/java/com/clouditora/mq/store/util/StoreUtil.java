package com.clouditora.mq.store.util;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.text.NumberFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
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

    public static long string2Long(String str) {
        return Long.parseLong(str);
    }

    public static String long2String(long l) {
        NumberFormat format = NumberFormat.getInstance();
        format.setMinimumIntegerDigits(20);
        format.setMaximumFractionDigits(0);
        format.setGroupingUsed(false);
        return format.format(l);
    }

    public static String timestamp2String(long timestamp) {
        Instant instant = Instant.ofEpochMilli(timestamp);
        LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
        return DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS").format(localDateTime);
    }
}
