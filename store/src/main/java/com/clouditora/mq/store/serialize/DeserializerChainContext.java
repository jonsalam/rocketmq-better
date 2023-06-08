package com.clouditora.mq.store.serialize;

import java.util.List;

public class DeserializerChainContext extends AbstractChainContext {

    public DeserializerChainContext(List<Serializer> serializers) {
        super(serializers);
    }

    public void next() {
        if (index >= serializers.size()) {
            index = 0;
            return;
        }
        Serializer serializer = serializers.get(index++);
        serializer.deserialize(this);
    }
}
