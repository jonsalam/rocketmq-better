package com.clouditora.mq.store.serializer;

import com.clouditora.mq.common.MessageConst;
import com.clouditora.mq.store.MessageEntity;

import java.nio.ByteBuffer;
import java.util.List;

public class ByteBufferSerializer extends AbstractSerializer<SerializerChain> {
    private ByteBuffer byteBuffer;

    @Override
    protected SerializerChain init(List<Serializer> serializers) {
        SerializerChain chain = new SerializerChain(serializers);
        this.byteBuffer = ByteBuffer.allocate(MessageConst.Maximum.MESSAGE_LENGTH);
        chain.setByteBuffer(this.byteBuffer);
        return chain;
    }

    public ByteBuffer serialize(long logOffset, int remainLength, MessageEntity message) throws SerializeException {
        chain.setLogOffset(logOffset);
        chain.setRemainLength(remainLength);
        chain.setMessage(message);
        // 计算消息的长度
        int length = chain.calcMessageLength();
        // 重置position, 重新指定limit
        byteBuffer.flip().limit(length);
        // 序列化
        chain.next();
        // 重置position, 指定limit
        byteBuffer.flip();
        return byteBuffer;
    }
}
