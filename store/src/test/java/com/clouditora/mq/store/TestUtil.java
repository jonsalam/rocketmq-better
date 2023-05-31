package com.clouditora.mq.store;

import com.clouditora.mq.common.message.MessageEntity;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

public class TestUtil {
    private static final byte[] messageBody = "Once, there was a chance for me!".getBytes(StandardCharsets.UTF_8);

    private static InetSocketAddress bornHost;
    private static InetSocketAddress storeHost;

    static {
        try {
            bornHost = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0);
            storeHost = new InetSocketAddress(InetAddress.getLocalHost(), 8123);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    public static MessageEntity buildMessage(String topic, byte[] messageBody) {
        MessageEntity msg = new MessageEntity();
        msg.setTopic(topic);
        msg.setTags("TAG1");
        msg.setKeys("Hello");
        msg.setBody(messageBody);
        msg.setKeys(String.valueOf(System.currentTimeMillis()));
        msg.setQueueId(0);
        msg.setSysFlag(0);
        msg.setBornTimestamp(System.currentTimeMillis());
        msg.setStoreHost(storeHost);
        msg.setBornHost(bornHost);
        return msg;
    }

    public static MessageEntity buildMessage(String topic) {
        return buildMessage(topic, messageBody);
    }

    public static MessageEntity buildMessage() {
        return buildMessage("FooBar", messageBody);
    }

    public static void sleep(int second) {
        try {
            TimeUnit.SECONDS.sleep(second);
        } catch (InterruptedException ignore) {
        }
    }
}
