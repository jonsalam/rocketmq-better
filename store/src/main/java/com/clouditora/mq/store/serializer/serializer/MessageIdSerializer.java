package com.clouditora.mq.store.serializer.serializer;

import com.clouditora.mq.common.message.MessageEntity;
import com.clouditora.mq.store.serializer.DeserializerChainContext;
import com.clouditora.mq.store.serializer.SerializeException;
import com.clouditora.mq.store.serializer.Serializer;
import com.clouditora.mq.store.serializer.SerializerChainContext;
import com.clouditora.mq.store.util.StoreUtil;

import java.nio.ByteBuffer;

public class MessageIdSerializer implements Serializer {
    @Override
    public void preSerializer(SerializerChainContext context) {
        MessageEntity message = context.getMessage();
        String messageId = createMessageId(message);
        message.setMessageId(messageId);
    }

    @Override
    public void serialize(SerializerChainContext context) throws SerializeException {
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
