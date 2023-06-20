package com.clouditora.mq.store.serialize.serializer;

import com.clouditora.mq.store.serialize.DeserializerChainContext;
import com.clouditora.mq.store.serialize.SerializeException;
import com.clouditora.mq.store.serialize.Serializer;
import com.clouditora.mq.store.serialize.SerializerChainContext;

import java.nio.ByteBuffer;

public class QueueOffsetSerializer implements Serializer {

    @Override
    public int fieldLength(SerializerChainContext context) {
        return 8;
    }

    @Override
    public void serialize(SerializerChainContext context) throws SerializeException {
        ByteBuffer byteBuffer = context.getByteBuffer();
        byteBuffer.putLong(context.getMessage().getQueuePosition());
        context.next();
    }

    @Override
    public void deserialize(DeserializerChainContext context) {
        ByteBuffer byteBuffer = context.getByteBuffer();
        long queueOffset = byteBuffer.getLong();
        context.getMessage().setQueuePosition(queueOffset);
        context.next();
    }
}
