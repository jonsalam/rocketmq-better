package com.clouditora.mq.store.serializer;

public interface Serializer {

    void preSerializer(SerializerChain chain);

    void serialize(SerializerChain chain) throws SerializeException;

    void deserialize(DeserializerChain chain);
}
