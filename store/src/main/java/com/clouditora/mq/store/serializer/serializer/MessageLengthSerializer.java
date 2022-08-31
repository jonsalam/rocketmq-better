package com.clouditora.mq.store.serializer.serializer;

import com.clouditora.mq.common.MessageConst;
import com.clouditora.mq.store.serializer.DeserializerChain;
import com.clouditora.mq.store.serializer.SerializeException;
import com.clouditora.mq.store.serializer.Serializer;
import com.clouditora.mq.store.serializer.SerializerChain;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;

@Slf4j
public class MessageLengthSerializer implements Serializer {

    @Override
    public void preSerializer(SerializerChain chain) {
        chain.addMessageLength(4);
    }

    @Override
    public void serialize(SerializerChain chain) throws SerializeException {
        int length = chain.getMessageLength();
        if (length > MessageConst.Maximum.MESSAGE_LENGTH) {
            log.error("message length too long: id={}, length={}", chain.getMessage().getTopic(), length);
            throw new SerializeException();
        }
        ByteBuffer byteBuffer = chain.getByteBuffer();
        byteBuffer.putInt(length);
        chain.getMessage().setMessageLength(length);
        chain.next();
    }

    @Override
    public void deserialize(DeserializerChain chain) {
        ByteBuffer byteBuffer = chain.getByteBuffer();
        chain.getMessage().setMessageLength(byteBuffer.getInt());
        chain.next();
    }
}
