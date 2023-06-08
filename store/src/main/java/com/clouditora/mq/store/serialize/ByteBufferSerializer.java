package com.clouditora.mq.store.serialize;

import com.clouditora.mq.common.MessageConst;
import com.clouditora.mq.common.message.MessageEntity;

import java.nio.ByteBuffer;
import java.util.List;

public class ByteBufferSerializer extends AbstractSerializer<SerializerChainContext> {
    private ByteBuffer byteBuffer;

    @Override
    protected SerializerChainContext init(List<Serializer> serializers) {
        this.byteBuffer = ByteBuffer.allocate(MessageConst.Maximum.MESSAGE_LENGTH);
        SerializerChainContext context = new SerializerChainContext(serializers);
        context.setByteBuffer(this.byteBuffer);
        return context;
    }

    public ByteBuffer serialize(long offset, int free, MessageEntity message) throws SerializeException {
        message.setCommitLogOffset(offset);
        super.context.setFree(free);
        super.context.setMessage(message);
        // 计算消息的长度
        int length = super.context.calcMessageLength();
        message.setMessageLength(length);
        // 重置position/limit
        this.byteBuffer.flip().limit(length);
        // 序列化
        super.context.next();
        // 重置position/limit
        this.byteBuffer.flip();
        return this.byteBuffer;
    }
}
