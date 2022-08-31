package com.clouditora.mq.store.serializer.serializer;

import com.clouditora.mq.common.MessageConst;
import com.clouditora.mq.store.MessageEntity;
import com.clouditora.mq.store.serializer.DeserializerChain;
import com.clouditora.mq.store.serializer.SerializeException;
import com.clouditora.mq.store.serializer.Serializer;
import com.clouditora.mq.store.serializer.SerializerChain;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;

@Slf4j
public class PropertiesSerializer implements Serializer {

    @Override
    public void preSerializer(SerializerChain chain) {
        MessageEntity message = chain.getMessage();
        chain.addMessageLength(2);
        byte[] propertyBytes = message.getPropertyBytes();
        if (propertyBytes != null) {
            chain.addMessageLength(propertyBytes.length);
        }
    }

    @Override
    public void serialize(SerializerChain chain) throws SerializeException {
        ByteBuffer byteBuffer = chain.getByteBuffer();
        byte[] propertyBytes = chain.getMessage().getPropertyBytes();
        if (propertyBytes == null) {
            byteBuffer.putShort((short) 0);
        } else {
            int length = propertyBytes.length;
            if (length > MessageConst.Maximum.PROPERTIES_LENGTH) {
                throw new SerializeException();
            }
            byteBuffer.putShort((short) length);
            byteBuffer.put(propertyBytes);
        }
        chain.next();
    }

    @Override
    public void deserialize(DeserializerChain chain) {
        ByteBuffer byteBuffer = chain.getByteBuffer();
        short length = byteBuffer.getShort();
        if (length == 0) {
            return;
        }
        byte[] bytes = new byte[length];
        byteBuffer.get(bytes);
        chain.getMessage().setPropertyBytes(bytes);
        chain.next();
    }
}
