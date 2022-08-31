package com.clouditora.mq.store.serializer.serializer;

import com.clouditora.mq.store.MessageEntity;
import com.clouditora.mq.store.serializer.DeserializerChain;
import com.clouditora.mq.store.serializer.SerializeException;
import com.clouditora.mq.store.serializer.Serializer;
import com.clouditora.mq.store.serializer.SerializerChain;
import com.clouditora.mq.store.util.StoreUtil;

import java.nio.ByteBuffer;

public class MessageIdSerializer implements Serializer {
    @Override
    public void preSerializer(SerializerChain chain) {

    }

    @Override
    public void serialize(SerializerChain chain) throws SerializeException {
        MessageEntity message = chain.getMessage();
        String messageId = createMessageId(message);
        message.setMessageId(messageId);
        chain.next();
    }

    @Override
    public void deserialize(DeserializerChain chain) {
        MessageEntity message = chain.getMessage();
        String msgId = createMessageId(message);
        message.setMessageId(msgId);
        chain.next();
    }

    private String createMessageId(MessageEntity message) {
        ByteBuffer storeHost = StoreUtil.socketAddress2ByteBuffer(message.getStoreHost());
        ByteBuffer byteBuffer = ByteBuffer.allocate(storeHost.limit() + 8);
        byteBuffer.put(storeHost.array());
        byteBuffer.putLong(message.getLogOffset());
        return StoreUtil.bytes2string(byteBuffer.array());
    }
}
