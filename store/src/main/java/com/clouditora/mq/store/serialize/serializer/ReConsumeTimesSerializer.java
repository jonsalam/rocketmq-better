package com.clouditora.mq.store.serialize.serializer;

import com.clouditora.mq.store.serialize.DeserializerChainContext;
import com.clouditora.mq.store.serialize.SerializeException;
import com.clouditora.mq.store.serialize.Serializer;
import com.clouditora.mq.store.serialize.SerializerChainContext;

import java.nio.ByteBuffer;

public class ReConsumeTimesSerializer implements Serializer {

    @Override
    public int fieldLength(SerializerChainContext context) {
        return 4;
    }

    @Override
    public void serialize(SerializerChainContext context) throws SerializeException {
        ByteBuffer byteBuffer = context.getByteBuffer();
        int times = context.getMessage().getReConsumeTimes();
        byteBuffer.putInt(times);
        context.next();
    }

    @Override
    public void deserialize(DeserializerChainContext context) {
        ByteBuffer byteBuffer = context.getByteBuffer();
        int times = byteBuffer.getInt();
        context.getMessage().setReConsumeTimes(times);
        context.next();
    }
}
