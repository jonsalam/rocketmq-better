package com.clouditora.mq.common.command.body;

import com.clouditora.mq.common.util.JsonUtil;

/**
 * @link org.apache.rocketmq.remoting.protocol.RemotingSerializable
 */
public class JsonBodyCommand {
    public byte[] encode() {
        return JsonUtil.toBytes(this);
    }

    public static <T> T decode(byte[] bytes, Class<T> clazz) {
        return JsonUtil.parse(bytes, clazz);
    }
}
