package com.clouditora.mq.store.serialize.serializer;

import com.clouditora.mq.store.serialize.DeserializerChainContext;
import com.clouditora.mq.store.serialize.SerializeException;
import com.clouditora.mq.store.serialize.Serializer;
import com.clouditora.mq.store.serialize.SerializerChainContext;

import java.nio.ByteBuffer;

public class TransactionOffsetSerializer implements Serializer {

    @Override
    public int fieldLength(SerializerChainContext context) {
        return 8;
    }

    @Override
    public void serialize(SerializerChainContext context) throws SerializeException {
        ByteBuffer byteBuffer = context.getByteBuffer();
        long transactionOffset = context.getMessage().getTransactionOffset();
        byteBuffer.putLong(transactionOffset);
        context.next();
    }

    @Override
    public void deserialize(DeserializerChainContext context) {
        ByteBuffer byteBuffer = context.getByteBuffer();
        long transactionOffset = byteBuffer.getLong();
        context.getMessage().setTransactionOffset(transactionOffset);
        context.next();
    }
}
