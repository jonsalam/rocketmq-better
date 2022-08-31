package com.clouditora.mq.store.serializer.serializer;

import com.clouditora.mq.common.MessageConst;
import com.clouditora.mq.store.MessageEntity;
import com.clouditora.mq.store.serializer.DeserializerChain;
import com.clouditora.mq.store.serializer.SerializeException;
import com.clouditora.mq.store.serializer.Serializer;
import com.clouditora.mq.store.serializer.SerializerChain;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

@Slf4j
public class TopicSerializer implements Serializer {
    private byte[] bytes;

    @Override
    public void preSerializer(SerializerChain chain) {
        MessageEntity message = chain.getMessage();
        bytes = message.getTopic().getBytes(StandardCharsets.UTF_8);
        chain.addMessageLength(1);
        chain.addMessageLength(bytes.length);
    }

    @Override
    public void serialize(SerializerChain chain) throws SerializeException {
        MessageEntity message = chain.getMessage();
        if (message.getTopic().length() > MessageConst.Maximum.TOPIC_LENGTH) {
            log.warn("topic length too long: topic={}, length={}", message.getTopic(), message.getTopic().length());
            throw new SerializeException();
        }
        ByteBuffer byteBuffer = chain.getByteBuffer();
        byteBuffer.put((byte) bytes.length);
        byteBuffer.put(bytes);
        chain.next();
    }

    @Override
    public void deserialize(DeserializerChain chain) {
        ByteBuffer byteBuffer = chain.getByteBuffer();
        byte length = byteBuffer.get();
        bytes = new byte[length];
        byteBuffer.get(bytes);
        chain.getMessage().setTopic(new String(bytes, StandardCharsets.UTF_8));
        chain.next();
    }
}
