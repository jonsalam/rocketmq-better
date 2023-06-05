package com.clouditora.mq.store.util;

import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.text.NumberFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;

public class StoreUtil {
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

    /**
     * @link org.apache.rocketmq.store.MappedFile#clean
     */
    public static void clean(ByteBuffer buffer) {
        if (buffer == null || buffer.capacity() == 0 || !buffer.isDirect()) {
            return;
        }
        invoke(invoke(viewed(buffer), "cleaner"), "clean");
    }

    private static ByteBuffer viewed(ByteBuffer buffer) {
        Method[] methods = buffer.getClass().getMethods();
        String methodName = Arrays.stream(methods)
                .map(Method::getName)
                .filter("attachment"::equals)
                .findFirst()
                .orElse("viewedBuffer");
        ByteBuffer viewedBuffer = (ByteBuffer) invoke(buffer, methodName);
        if (viewedBuffer == null) {
            return buffer;
        } else {
            return viewed(viewedBuffer);
        }
    }

    private static Object invoke(Object target, String methodName, Class<?>... args) {
        try {
            Method method = getMethod(target, methodName, args);
            method.setAccessible(true);
            return method.invoke(target);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static Method getMethod(Object target, String methodName, Class<?>[] args) throws NoSuchMethodException {
        try {
            return target.getClass().getMethod(methodName, args);
        } catch (NoSuchMethodException e) {
            return target.getClass().getDeclaredMethod(methodName, args);
        }
    }
}
