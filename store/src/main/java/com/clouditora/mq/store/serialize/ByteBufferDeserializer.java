package com.clouditora.mq.store.serialize;

import com.clouditora.mq.common.message.MessageEntity;

import java.nio.ByteBuffer;
import java.util.List;

public class ByteBufferDeserializer extends AbstractSerializer<DeserializerChainContext> {
    @Override
    protected DeserializerChainContext init(List<Serializer> serializers) {
        return new DeserializerChainContext(serializers);
    }

    public MessageEntity deserialize(ByteBuffer byteBuffer) {
        context.setByteBuffer(byteBuffer);
        context.setMessage(new MessageEntity());
        // 序列化
        context.next();
        return context.getMessage();
    }
}
