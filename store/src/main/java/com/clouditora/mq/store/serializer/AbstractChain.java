package com.clouditora.mq.store.serializer;

import com.clouditora.mq.store.MessageEntity;
import lombok.Getter;
import lombok.Setter;

import java.nio.ByteBuffer;
import java.util.List;

@Getter
public abstract class AbstractChain {
    protected int index;
    protected List<Serializer> serializers;
    @Setter
    protected ByteBuffer byteBuffer;
    @Setter
    protected MessageEntity message;

    public AbstractChain(List<Serializer> serializers) {
        this.index = 0;
        this.serializers = serializers;
    }
}
