package com.clouditora.mq.store.serializer;

import com.clouditora.mq.common.MessageConst;
import com.clouditora.mq.store.MessageEntity;

import java.nio.ByteBuffer;
import java.util.List;

public class ByteBufferSerializer extends AbstractSerializer<SerializerChainContext> {
    private ByteBuffer byteBuffer;

    @Override
    protected SerializerChainContext init(List<Serializer> serializers) {
        SerializerChainContext chain = new SerializerChainContext(serializers);
        this.byteBuffer = ByteBuffer.allocate(MessageConst.Maximum.MESSAGE_LENGTH);
        chain.setByteBuffer(this.byteBuffer);
        return chain;
    }

    public ByteBuffer serialize(long logOffset, int remainLength, MessageEntity message) throws SerializeException {
        super.chain.setLogOffset(logOffset);
        super.chain.setRemainLength(remainLength);
        super.chain.setMessage(message);
        // 计算消息的长度
        int length = super.chain.calcMessageLength();
        // 重置position/limit
        this.byteBuffer.flip().limit(length);
        // 序列化
        super.chain.next();
        // 重置position/limit
        this.byteBuffer.flip();
        return this.byteBuffer;
    }
}
