package com.clouditora.mq.store.serialize;

import com.clouditora.mq.common.message.MessageEntity;
import lombok.Getter;
import lombok.Setter;

import java.nio.ByteBuffer;
import java.util.List;

@Getter
public abstract class AbstractChainContext {
    protected int index;
    protected List<Serializer> serializers;
    @Setter
    protected ByteBuffer byteBuffer;
    @Setter
    protected MessageEntity message;

    public AbstractChainContext(List<Serializer> serializers) {
        this.index = 0;
        this.serializers = serializers;
    }

    public void reset() {
        this.index = 0;
    }
}
