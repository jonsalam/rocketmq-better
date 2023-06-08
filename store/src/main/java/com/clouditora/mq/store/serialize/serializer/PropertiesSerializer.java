package com.clouditora.mq.store.serialize.serializer;

import com.clouditora.mq.common.MessageConst;
import com.clouditora.mq.common.message.MessageEntity;
import com.clouditora.mq.common.util.MessageUtil;
import com.clouditora.mq.store.serialize.DeserializerChainContext;
import com.clouditora.mq.store.serialize.SerializeException;
import com.clouditora.mq.store.serialize.Serializer;
import com.clouditora.mq.store.serialize.SerializerChainContext;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;

@Slf4j
public class PropertiesSerializer implements Serializer {
    private byte[] propertyBytes;

    @Override
    public int fieldLength(SerializerChainContext context) {
        MessageEntity message = context.getMessage();
        this.propertyBytes = MessageUtil.properties2Bytes(message.getProperties());
        if (propertyBytes == null) {
            return 2;
        } else {
            return 2 + propertyBytes.length;
        }
    }

    @Override
    public void serialize(SerializerChainContext context) throws SerializeException {
        ByteBuffer byteBuffer = context.getByteBuffer();
        int length = this.propertyBytes == null ? 0 : this.propertyBytes.length;
        if (length > MessageConst.Maximum.PROPERTIES_LENGTH) {
            throw new SerializeException();
        }
        byteBuffer.putShort((short) length);
        if (length > 0) {
            byteBuffer.put(this.propertyBytes);
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
