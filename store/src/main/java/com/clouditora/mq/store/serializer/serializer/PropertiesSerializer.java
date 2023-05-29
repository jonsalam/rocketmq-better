package com.clouditora.mq.store.serializer.serializer;

import com.clouditora.mq.common.MessageConst;
import com.clouditora.mq.store.MessageEntity;
import com.clouditora.mq.store.serializer.DeserializerChainContext;
import com.clouditora.mq.store.serializer.SerializeException;
import com.clouditora.mq.store.serializer.Serializer;
import com.clouditora.mq.store.serializer.SerializerChainContext;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;

@Slf4j
public class PropertiesSerializer implements Serializer {

    @Override
    public void preSerializer(SerializerChainContext context) {
        MessageEntity message = context.getMessage();
        context.addMessageLength(2);
        byte[] propertyBytes = message.getPropertyBytes();
        if (propertyBytes != null) {
            context.addMessageLength(propertyBytes.length);
        }
    }

    @Override
    public void serialize(SerializerChainContext context) throws SerializeException {
        ByteBuffer byteBuffer = context.getByteBuffer();
        byte[] propertyBytes = context.getMessage().getPropertyBytes();
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
        context.next();
    }

    @Override
    public void deserialize(DeserializerChainContext context) {
        ByteBuffer byteBuffer = context.getByteBuffer();
        short length = byteBuffer.getShort();
        if (length == 0) {
            return;
        }
        byte[] bytes = new byte[length];
        byteBuffer.get(bytes);
        context.getMessage().setPropertyBytes(bytes);
        context.next();
    }
}
