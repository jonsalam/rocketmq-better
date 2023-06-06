package com.clouditora.mq.store.serialize;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.nio.ByteBuffer;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@Data
public class EndOfFileException extends SerializeException {
    private final ByteBuffer byteBuffer;
    private final int free;
    private final int length;

    public EndOfFileException(ByteBuffer byteBuffer, int free, int length) {
        this.byteBuffer = byteBuffer;
        this.free = free;
        this.length = length;
    }
}
