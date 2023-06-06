package com.clouditora.mq.store.serialize.serializer;

import com.clouditora.mq.common.MessageConst;
import com.clouditora.mq.store.serialize.DeserializerChainContext;
import com.clouditora.mq.store.serialize.SerializeException;
import com.clouditora.mq.store.serialize.Serializer;
import com.clouditora.mq.store.serialize.SerializerChainContext;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;

@Slf4j
public class MessageLengthSerializer implements Serializer {

    @Override
    public int fieldLength(SerializerChainContext context) {
        return 4;
    }

    @Override
    public void serialize(SerializerChainContext context) throws SerializeException {
        int length = context.getMessage().getMessageLength();
        if (length > MessageConst.Maximum.MESSAGE_LENGTH) {
            log.error("message length too long: id={}, length={}", context.getMessage().getTopic(), length);
            throw new SerializeException();
        }
        ByteBuffer byteBuffer = context.getByteBuffer();
        byteBuffer.putInt(length);
        context.next();
    }

    @Override
    public void deserialize(DeserializerChainContext context) {
        ByteBuffer byteBuffer = context.getByteBuffer();
        context.getMessage().setMessageLength(byteBuffer.getInt());
        context.next();
    }
}
