package com.clouditora.mq.store.serializer;

public interface Serializer {

    void preSerializer(SerializerChainContext context);

    void serialize(SerializerChainContext context) throws SerializeException;

    void deserialize(DeserializerChainContext context);
}
