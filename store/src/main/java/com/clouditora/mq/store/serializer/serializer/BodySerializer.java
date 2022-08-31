package com.clouditora.mq.store.serializer.serializer;

import com.clouditora.mq.store.MessageEntity;
import com.clouditora.mq.store.serializer.DeserializerChain;
import com.clouditora.mq.store.serializer.SerializeException;
import com.clouditora.mq.store.serializer.Serializer;
import com.clouditora.mq.store.serializer.SerializerChain;

import java.nio.ByteBuffer;

public class BodySerializer implements Serializer {

    @Override
    public void preSerializer(SerializerChain chain) {
        MessageEntity message = chain.getMessage();
        chain.addMessageLength(4);
        byte[] body = message.getBody();
        if (body != null) {
            chain.addMessageLength(body.length);
        }
    }

    @Override
    public void serialize(SerializerChain chain) throws SerializeException {
        ByteBuffer byteBuffer = chain.getByteBuffer();
        byte[] body = chain.getMessage().getBody();
        if (body == null) {
            byteBuffer.putInt(0);
        } else {
            byteBuffer.putInt(body.length);
            byteBuffer.put(body);
        }
        chain.next();
    }

    @Override
    public void deserialize(DeserializerChain chain) {
        ByteBuffer byteBuffer = chain.getByteBuffer();
        int length = byteBuffer.getInt();
        if (length == 0) {
            return;
        }
        byte[] body = new byte[length];
        byteBuffer.get(body);
        chain.getMessage().setBody(body);
        chain.next();
    }
}
