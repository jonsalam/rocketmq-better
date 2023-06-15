package com.clouditora.mq.store.serialize;

import java.util.List;

public class DeserializerChainContext extends AbstractChainContext {

    public DeserializerChainContext(List<Serializer> serializers) {
        super(serializers);
    }

    public void next() {
        if (super.index >= super.serializers.size()) {
            reset();
            return;
        }
        Serializer serializer = super.serializers.get(super.index++);
        serializer.deserialize(this);
    }
}
