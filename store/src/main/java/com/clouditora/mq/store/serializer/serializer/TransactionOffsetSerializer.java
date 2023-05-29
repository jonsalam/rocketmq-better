package com.clouditora.mq.store.serializer.serializer;

import com.clouditora.mq.store.serializer.DeserializerChainContext;
import com.clouditora.mq.store.serializer.SerializeException;
import com.clouditora.mq.store.serializer.Serializer;
import com.clouditora.mq.store.serializer.SerializerChainContext;

import java.nio.ByteBuffer;

public class TransactionOffsetSerializer implements Serializer {

    @Override
    public void preSerializer(SerializerChainContext context) {
        context.addMessageLength(8);
    }

    @Override
    public void serialize(SerializerChainContext context) throws SerializeException {
        ByteBuffer byteBuffer = context.getByteBuffer();
        byteBuffer.putLong(context.getMessage().getTransactionOffset());
        context.next();
    }

    @Override
    public void deserialize(DeserializerChainContext context) {
        ByteBuffer byteBuffer = context.getByteBuffer();
        context.getMessage().setTransactionOffset(byteBuffer.getLong());
        context.next();
    }
}
