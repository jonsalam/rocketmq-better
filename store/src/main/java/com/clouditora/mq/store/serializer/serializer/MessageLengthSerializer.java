package com.clouditora.mq.store.serializer.serializer;

import com.clouditora.mq.common.MessageConst;
import com.clouditora.mq.store.serializer.DeserializerChainContext;
import com.clouditora.mq.store.serializer.SerializeException;
import com.clouditora.mq.store.serializer.Serializer;
import com.clouditora.mq.store.serializer.SerializerChainContext;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;

@Slf4j
public class MessageLengthSerializer implements Serializer {

    @Override
    public void preSerializer(SerializerChainContext context) {
        context.addMessageLength(4);
    }

    @Override
    public void serialize(SerializerChainContext context) throws SerializeException {
        int length = context.getMessageLength();
        if (length > MessageConst.Maximum.MESSAGE_LENGTH) {
            log.error("message length too long: id={}, length={}", context.getMessage().getTopic(), length);
            throw new SerializeException();
        }
        ByteBuffer byteBuffer = context.getByteBuffer();
        byteBuffer.putInt(length);
        context.getMessage().setMessageLength(length);
        context.next();
    }

    @Override
    public void deserialize(DeserializerChainContext context) {
        ByteBuffer byteBuffer = context.getByteBuffer();
        context.getMessage().setMessageLength(byteBuffer.getInt());
        context.next();
    }
}
