package com.clouditora.mq.store.serializer.serializer;

import com.clouditora.mq.common.MessageConst;
import com.clouditora.mq.store.MessageEntity;
import com.clouditora.mq.store.serializer.DeserializerChain;
import com.clouditora.mq.store.serializer.SerializeException;
import com.clouditora.mq.store.serializer.Serializer;
import com.clouditora.mq.store.serializer.SerializerChain;

import java.nio.ByteBuffer;

public class MagicCodeSerializer implements Serializer {

    @Override
    public void preSerializer(SerializerChain chain) {
        chain.addMessageLength(4);
    }

    @Override
    public void serialize(SerializerChain chain) throws SerializeException {
        ByteBuffer byteBuffer = chain.getByteBuffer();
        // Determines whether there is sufficient free space
        if (chain.getMessageLength() + MessageConst.Maximum.MIN_BLANK_LENGTH > chain.getRemainLength()) {
            byteBuffer.flip().limit(8);
            // 1 TOTAL SIZE
            byteBuffer.putInt(chain.getRemainLength());
            // 2 MAGIC CODE
            byteBuffer.putInt(MessageConst.MagicCode.BLANK);
            throw new EndOfFileException(chain.getRemainLength(), chain.getMessageLength(), byteBuffer);
        }
        byteBuffer.putInt(MessageConst.MagicCode.MESSAGE);
        chain.getMessage().setMagicCode(MessageConst.MagicCode.MESSAGE);
        chain.next();
    }

    @Override
    public void deserialize(DeserializerChain chain) {
        ByteBuffer byteBuffer = chain.getByteBuffer();
        MessageEntity message = chain.getMessage();
        int magicCode = byteBuffer.getInt();
        if (magicCode == MessageConst.MagicCode.MESSAGE) {
            message.setMagicCode(magicCode);
            chain.next();
        } else {
            message.setMagicCode(MessageConst.MagicCode.BLANK);
        }
    }
}
