package com.clouditora.mq.store.serializer.serializer;

import com.clouditora.mq.common.message.MessageEntity;
import com.clouditora.mq.store.serializer.DeserializerChainContext;
import com.clouditora.mq.store.serializer.SerializeException;
import com.clouditora.mq.store.serializer.Serializer;
import com.clouditora.mq.store.serializer.SerializerChainContext;

import java.nio.ByteBuffer;

public class BodySerializer implements Serializer {

    @Override
    public void preSerializer(SerializerChainContext context) {
        MessageEntity message = context.getMessage();
        context.addMessageLength(4);
        byte[] body = message.getBody();
        if (body != null) {
            context.addMessageLength(body.length);
        }
    }

    @Override
    public void serialize(SerializerChainContext context) throws SerializeException {
        ByteBuffer byteBuffer = context.getByteBuffer();
        byte[] body = context.getMessage().getBody();
        if (body == null) {
            byteBuffer.putInt(0);
        } else {
            byteBuffer.putInt(body.length);
            byteBuffer.put(body);
        }
        context.next();
    }

    @Override
    public void deserialize(DeserializerChainContext context) {
        ByteBuffer byteBuffer = context.getByteBuffer();
        int length = byteBuffer.getInt();
        if (length == 0) {
            return;
        }
        byte[] body = new byte[length];
        byteBuffer.get(body);
        context.getMessage().setBody(body);
        context.next();
    }
}
