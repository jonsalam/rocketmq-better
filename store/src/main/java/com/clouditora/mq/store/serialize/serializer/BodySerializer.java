package com.clouditora.mq.store.serialize.serializer;

import com.clouditora.mq.common.message.MessageEntity;
import com.clouditora.mq.store.serialize.DeserializerChainContext;
import com.clouditora.mq.store.serialize.SerializeException;
import com.clouditora.mq.store.serialize.Serializer;
import com.clouditora.mq.store.serialize.SerializerChainContext;

import java.nio.ByteBuffer;

public class BodySerializer implements Serializer {

    @Override
    public int fieldLength(SerializerChainContext context) {
        MessageEntity message = context.getMessage();
        byte[] body = message.getBody();
        if (body == null) {
            return 4;
        } else {
            return 4 + body.length;
        }
    }

    @Override
    public void serialize(SerializerChainContext context) throws SerializeException {
        ByteBuffer byteBuffer = context.getByteBuffer();
        byte[] body = context.getMessage().getBody();
        byteBuffer.putInt(body.length);
        if (body.length > 0) {
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
