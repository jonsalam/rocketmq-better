package com.clouditora.mq.store.serialize;

public interface Serializer {

    int fieldLength(SerializerChainContext context);

    void serialize(SerializerChainContext context) throws SerializeException;

    void deserialize(DeserializerChainContext context);
}
