package com.clouditora.mq.store.serialize.serializer;

import com.clouditora.mq.common.MessageConst;
import com.clouditora.mq.common.message.MessageEntity;
import com.clouditora.mq.store.serialize.*;

import java.nio.ByteBuffer;

public class MagicCodeSerializer implements Serializer {

    @Override
    public int fieldLength(SerializerChainContext context) {
        return 4;
    }

    @Override
    public void serialize(SerializerChainContext context) throws SerializeException {
        ByteBuffer byteBuffer = context.getByteBuffer();
        MessageEntity message = context.getMessage();
        // Determines whether there is sufficient free space
        if (message.getMessageLength() + MessageConst.Maximum.MIN_BLANK_LENGTH > context.getFree()) {
            byteBuffer.flip().limit(8);
            // 1 TOTAL SIZE
            byteBuffer.putInt(context.getFree());
            // 2 MAGIC CODE
            byteBuffer.putInt(MessageConst.MagicCode.BLANK);
            throw new EndOfFileException(byteBuffer, context.getFree(), message.getMessageLength());
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
        message.setMagicCode(magicCode);
        if (magicCode == MessageConst.MagicCode.MESSAGE) {
            context.next();
        }
    }
}
