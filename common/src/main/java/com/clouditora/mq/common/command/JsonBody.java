package com.clouditora.mq.common.command;

import com.clouditora.mq.common.util.JsonUtil;

/**
 * @link org.apache.rocketmq.remoting.protocol.RemotingSerializable
 */
public class JsonBody {
    public byte[] encode() {
        return JsonUtil.toBytes(this);
    }

    public static <T> T decode(byte[] bytes, Class<T> clazz) {
        return JsonUtil.parse(bytes, clazz);
    }
}
