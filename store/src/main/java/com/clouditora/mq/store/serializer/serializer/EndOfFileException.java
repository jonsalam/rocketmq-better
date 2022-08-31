package com.clouditora.mq.store.serializer.serializer;

import com.clouditora.mq.store.serializer.SerializeException;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.nio.ByteBuffer;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@Data
public class EndOfFileException extends SerializeException {
    private final int remainLength;
    private final int messageLength;
    private final ByteBuffer byteBuffer;

    public EndOfFileException(int remainLength, int messageLength, ByteBuffer byteBuffer) {
        this.remainLength = remainLength;
        this.messageLength = messageLength;
        this.byteBuffer = byteBuffer;
    }
}
