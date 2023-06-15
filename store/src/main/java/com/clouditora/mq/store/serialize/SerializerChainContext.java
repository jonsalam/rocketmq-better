package com.clouditora.mq.store.serialize;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@EqualsAndHashCode(callSuper = true)
@Setter
@Getter
public class SerializerChainContext extends AbstractChainContext {
    /**
     * remain length of mapped file
     */
    private int free;

    public SerializerChainContext(List<Serializer> serializers) {
        super(serializers);
    }

    public int calcMessageLength() {
        for (Serializer serializer : super.serializers) {
            serializer.fieldLength(this);
        }
        return super.serializers.stream().mapToInt(e -> e.fieldLength(this)).sum();
    }

    public void next() throws SerializeException {
        if (super.index >= super.serializers.size()) {
            reset();
            return;
        }
        Serializer serializer = super.serializers.get(super.index++);
        serializer.serialize(this);
    }

}
