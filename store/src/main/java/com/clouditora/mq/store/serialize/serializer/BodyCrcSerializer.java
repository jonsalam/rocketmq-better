package com.clouditora.mq.store.serialize.serializer;

import com.clouditora.mq.store.serialize.DeserializerChainContext;
import com.clouditora.mq.store.serialize.SerializeException;
import com.clouditora.mq.store.serialize.Serializer;
import com.clouditora.mq.store.serialize.SerializerChainContext;

import java.nio.ByteBuffer;

public class BodyCrcSerializer implements Serializer {

    @Override
    public int fieldLength(SerializerChainContext context) {
        return 4;
    }

    @Override
    public void serialize(SerializerChainContext context) throws SerializeException {
        ByteBuffer byteBuffer = context.getByteBuffer();
        byteBuffer.putInt(context.getMessage().getBodyCrc());
        context.next();
    }

    @Override
    public void deserialize(DeserializerChainContext context) {
        ByteBuffer byteBuffer = context.getByteBuffer();
        int bodyCrc = byteBuffer.getInt();
        context.getMessage().setBodyCrc(bodyCrc);
        context.next();
    }
}
