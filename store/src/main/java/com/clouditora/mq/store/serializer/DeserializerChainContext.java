package com.clouditora.mq.store.serializer;

import java.util.List;

public class DeserializerChainContext extends AbstractChainContext {

    public DeserializerChainContext(List<Serializer> serializers) {
        super(serializers);
    }

    public void next() {
        if (index >= serializers.size()) {
            return;
        }
        Serializer serializer = serializers.get(index);
        index++;
        serializer.deserialize(this);
    }
}