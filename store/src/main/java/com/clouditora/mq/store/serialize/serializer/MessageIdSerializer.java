package com.clouditora.mq.store.serialize.serializer;

import com.clouditora.mq.common.message.MessageEntity;
import com.clouditora.mq.store.serialize.DeserializerChainContext;
import com.clouditora.mq.store.serialize.SerializeException;
import com.clouditora.mq.store.serialize.Serializer;
import com.clouditora.mq.store.serialize.SerializerChainContext;
import com.clouditora.mq.store.util.StoreUtil;

import java.nio.ByteBuffer;

public class MessageIdSerializer implements Serializer {
    @Override
    public int fieldLength(SerializerChainContext context) {
        return 0;
    }

    @Override
    public void serialize(SerializerChainContext context) throws SerializeException {
        MessageEntity message = context.getMessage();
        String messageId = createMessageId(message);
        message.setMessageId(messageId);
        context.next();
    }

    @Override
    public void deserialize(DeserializerChainContext context) {
        MessageEntity message = context.getMessage();
        String msgId = createMessageId(message);
        message.setMessageId(msgId);
        context.next();
    }

    private String createMessageId(MessageEntity message) {
        ByteBuffer storeHost = StoreUtil.socketAddress2ByteBuffer(message.getStoreHost());
        ByteBuffer byteBuffer = ByteBuffer.allocate(storeHost.limit() + 8);
        byteBuffer.put(storeHost.array());
        byteBuffer.putLong(message.getCommitLogOffset());
        return StoreUtil.bytes2string(byteBuffer.array());
    }
}
