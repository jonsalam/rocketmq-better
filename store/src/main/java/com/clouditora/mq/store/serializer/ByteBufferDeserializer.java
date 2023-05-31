package com.clouditora.mq.store.serializer;

import com.clouditora.mq.common.message.MessageEntity;

import java.nio.ByteBuffer;
import java.util.List;

public class ByteBufferDeserializer extends AbstractSerializer<DeserializerChainContext> {
    @Override
    protected DeserializerChainContext init(List<Serializer> serializers) {
        return new DeserializerChainContext(serializers);
    }

    public MessageEntity deserialize(ByteBuffer byteBuffer) {
        chain.setByteBuffer(byteBuffer);
        chain.setMessage(new MessageEntity());
        // 序列化
        chain.next();
        return chain.getMessage();
    }
}
