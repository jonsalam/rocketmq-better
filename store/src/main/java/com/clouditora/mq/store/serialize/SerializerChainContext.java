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
        for (Serializer serializer : this.serializers) {
            serializer.fieldLength(this);
        }
        return this.serializers.stream().mapToInt(e -> e.fieldLength(this)).sum();
    }

    public void next() throws SerializeException {
        if (this.index >= this.serializers.size()) {
            this.index = 0;
            return;
        }
        Serializer serializer = this.serializers.get(index++);
        serializer.serialize(this);
    }

}
