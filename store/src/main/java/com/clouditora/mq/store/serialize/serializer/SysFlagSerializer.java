package com.clouditora.mq.store.serialize.serializer;

import com.clouditora.mq.store.serialize.DeserializerChainContext;
import com.clouditora.mq.store.serialize.SerializeException;
import com.clouditora.mq.store.serialize.Serializer;
import com.clouditora.mq.store.serialize.SerializerChainContext;

import java.nio.ByteBuffer;

public class SysFlagSerializer implements Serializer {

    @Override
    public int fieldLength(SerializerChainContext context) {
        return 4;
    }

    @Override
    public void serialize(SerializerChainContext context) throws SerializeException {
        ByteBuffer byteBuffer = context.getByteBuffer();
        int sysFlag = context.getMessage().getSysFlag();
        byteBuffer.putInt(sysFlag);
        context.next();
    }

    @Override
    public void deserialize(DeserializerChainContext context) {
        ByteBuffer byteBuffer = context.getByteBuffer();
        int sysFlag = byteBuffer.getInt();
        context.getMessage().setSysFlag(sysFlag);
        context.next();
    }
}
