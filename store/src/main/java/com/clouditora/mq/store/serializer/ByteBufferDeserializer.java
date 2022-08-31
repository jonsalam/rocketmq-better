package com.clouditora.mq.store.serializer;

import com.clouditora.mq.store.MessageEntity;

import java.nio.ByteBuffer;
import java.util.List;

public class ByteBufferDeserializer extends AbstractSerializer<DeserializerChain> {
    @Override
    protected DeserializerChain init(List<Serializer> serializers) {
        return new DeserializerChain(serializers);
    }

    public MessageEntity deserialize(ByteBuffer byteBuffer) {
        chain.setByteBuffer(byteBuffer);
        chain.setMessage(new MessageEntity());
        // 序列化
        chain.next();
        return chain.getMessage();
    }
}
