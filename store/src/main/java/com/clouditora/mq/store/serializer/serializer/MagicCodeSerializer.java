package com.clouditora.mq.store.serializer.serializer;

import com.clouditora.mq.common.MessageConst;
import com.clouditora.mq.store.MessageEntity;
import com.clouditora.mq.store.serializer.DeserializerChainContext;
import com.clouditora.mq.store.serializer.SerializeException;
import com.clouditora.mq.store.serializer.Serializer;
import com.clouditora.mq.store.serializer.SerializerChainContext;

import java.nio.ByteBuffer;

public class MagicCodeSerializer implements Serializer {

    @Override
    public void preSerializer(SerializerChainContext context) {
        context.addMessageLength(4);
    }

    @Override
    public void serialize(SerializerChainContext context) throws SerializeException {
        ByteBuffer byteBuffer = context.getByteBuffer();
        // Determines whether there is sufficient free space
        if (context.getMessageLength() + MessageConst.Maximum.MIN_BLANK_LENGTH > context.getRemainLength()) {
            byteBuffer.flip().limit(8);
            // 1 TOTAL SIZE
            byteBuffer.putInt(context.getRemainLength());
            // 2 MAGIC CODE
            byteBuffer.putInt(MessageConst.MagicCode.BLANK);
            throw new EndOfFileException(context.getRemainLength(), context.getMessageLength(), byteBuffer);
        }
        byteBuffer.putInt(MessageConst.MagicCode.MESSAGE);
        context.getMessage().setMagicCode(MessageConst.MagicCode.MESSAGE);
        context.next();
    }

    @Override
    public void deserialize(DeserializerChainContext context) {
        ByteBuffer byteBuffer = context.getByteBuffer();
        MessageEntity message = context.getMessage();
        int magicCode = byteBuffer.getInt();
        if (magicCode == MessageConst.MagicCode.MESSAGE) {
            message.setMagicCode(magicCode);
            context.next();
        } else {
            message.setMagicCode(MessageConst.MagicCode.BLANK);
        }
    }
}
