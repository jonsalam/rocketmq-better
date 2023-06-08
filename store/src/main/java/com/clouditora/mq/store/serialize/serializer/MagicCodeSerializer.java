package com.clouditora.mq.store.serialize.serializer;

import com.clouditora.mq.common.MessageConst;
import com.clouditora.mq.common.constant.MagicCode;
import com.clouditora.mq.common.message.MessageEntity;
import com.clouditora.mq.store.serialize.*;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;

@Slf4j
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
            byteBuffer.putInt(MagicCode.BLANK.getCode());
            throw new EndOfFileException(byteBuffer, context.getFree(), message.getMessageLength());
        }
        context.getMessage().setMagicCode(MagicCode.MESSAGE);
        byteBuffer.putInt(MagicCode.MESSAGE.getCode());
        context.next();
    }

    @Override
    public void deserialize(DeserializerChainContext context) {
        ByteBuffer byteBuffer = context.getByteBuffer();
        MessageEntity message = context.getMessage();
        int magicCode = byteBuffer.getInt();
        if (magicCode == MagicCode.BLANK.getCode()) {
            message.setMagicCode(MagicCode.BLANK);
        } else if (magicCode == MagicCode.MESSAGE.getCode()) {
            message.setMagicCode(MagicCode.MESSAGE);
            context.next();
        } else {
            message.setMagicCode(MagicCode.ERROR);
            log.error("found illegal magic code: position={}", byteBuffer.position());
        }
    }
}
