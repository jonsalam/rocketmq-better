package com.clouditora.mq.store.serialize.serializer;

import com.clouditora.mq.common.MessageConst;
import com.clouditora.mq.common.message.MessageEntity;
import com.clouditora.mq.store.serialize.DeserializerChainContext;
import com.clouditora.mq.store.serialize.SerializeException;
import com.clouditora.mq.store.serialize.Serializer;
import com.clouditora.mq.store.serialize.SerializerChainContext;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

@Slf4j
public class TopicSerializer implements Serializer {
    private byte[] bytes;

    @Override
    public int fieldLength(SerializerChainContext context) {
        MessageEntity message = context.getMessage();
        this.bytes = message.getTopic().getBytes(StandardCharsets.UTF_8);
        return 1 + this.bytes.length;
    }

    @Override
    public void serialize(SerializerChainContext context) throws SerializeException {
        MessageEntity message = context.getMessage();
        if (message.getTopic().length() > MessageConst.Maximum.TOPIC_LENGTH) {
            log.warn("topic length too long: topic={}, length={}", message.getTopic(), message.getTopic().length());
            throw new SerializeException();
        }
        ByteBuffer byteBuffer = context.getByteBuffer();
        byteBuffer.put((byte) this.bytes.length);
        byteBuffer.put(this.bytes);
        context.next();
    }

    @Override
    public void deserialize(DeserializerChainContext context) {
        ByteBuffer byteBuffer = context.getByteBuffer();
        byte length = byteBuffer.get();
        this.bytes = new byte[length];
        byteBuffer.get(this.bytes);
        context.getMessage().setTopic(new String(this.bytes, StandardCharsets.UTF_8));
        context.next();
    }
}
