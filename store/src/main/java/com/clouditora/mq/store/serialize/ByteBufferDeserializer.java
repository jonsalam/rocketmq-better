package com.clouditora.mq.store.serialize;

import com.clouditora.mq.common.constant.MagicCode;
import com.clouditora.mq.common.message.MessageEntity;

import java.nio.ByteBuffer;
import java.util.List;

public class ByteBufferDeserializer extends AbstractSerializer<DeserializerChainContext> {
    @Override
    protected DeserializerChainContext init(List<Serializer> serializers) {
        return new DeserializerChainContext(serializers);
    }

    /**
     * @link org.apache.rocketmq.store.CommitLog#checkMessageAndReturnSize(java.nio.ByteBuffer, boolean, boolean)
     */
    public MessageEntity deserialize(ByteBuffer byteBuffer) {
        MessageEntity message = new MessageEntity();
        message.setMagicCode(MagicCode.BLANK);

        context.setByteBuffer(byteBuffer);
        context.setMessage(message);
        context.next();
        return context.getMessage();
    }
}
