package com.clouditora.mq.store.serializer.serializer;

import com.clouditora.mq.common.MessageConst;
import com.clouditora.mq.common.message.MessageEntity;
import com.clouditora.mq.store.serializer.DeserializerChainContext;
import com.clouditora.mq.store.serializer.SerializeException;
import com.clouditora.mq.store.serializer.Serializer;
import com.clouditora.mq.store.serializer.SerializerChainContext;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

@Slf4j
public class TopicSerializer implements Serializer {
    private byte[] bytes;

    @Override
    public void preSerializer(SerializerChainContext context) {
        MessageEntity message = context.getMessage();
        bytes = message.getTopic().getBytes(StandardCharsets.UTF_8);
        context.addMessageLength(1);
        context.addMessageLength(bytes.length);
    }

    @Override
    public void serialize(SerializerChainContext context) throws SerializeException {
        MessageEntity message = context.getMessage();
        if (message.getTopic().length() > MessageConst.Maximum.TOPIC_LENGTH) {
            log.warn("topic length too long: topic={}, length={}", message.getTopic(), message.getTopic().length());
            throw new SerializeException();
        }
        ByteBuffer byteBuffer = context.getByteBuffer();
        byteBuffer.put((byte) bytes.length);
        byteBuffer.put(bytes);
        context.next();
    }

    @Override
    public void deserialize(DeserializerChainContext context) {
        ByteBuffer byteBuffer = context.getByteBuffer();
        byte length = byteBuffer.get();
        bytes = new byte[length];
        byteBuffer.get(bytes);
        context.getMessage().setTopic(new String(bytes, StandardCharsets.UTF_8));
        context.next();
    }
}
