package com.clouditora.mq.store.serializer.serializer;

import com.clouditora.mq.store.serializer.DeserializerChain;
import com.clouditora.mq.store.serializer.SerializeException;
import com.clouditora.mq.store.serializer.Serializer;
import com.clouditora.mq.store.serializer.SerializerChain;

import java.nio.ByteBuffer;

public class BornTimestampSerializer implements Serializer {

    @Override
    public void preSerializer(SerializerChain chain) {
        chain.addMessageLength(8);
    }

    @Override
    public void serialize(SerializerChain chain) throws SerializeException {
        ByteBuffer byteBuffer = chain.getByteBuffer();
        byteBuffer.putLong(chain.getMessage().getBornTimestamp());
        chain.next();
    }

    @Override
    public void deserialize(DeserializerChain chain) {
        ByteBuffer byteBuffer = chain.getByteBuffer();
        chain.getMessage().setBornTimestamp(byteBuffer.getLong());
        chain.next();
    }
}
