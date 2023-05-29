package com.clouditora.mq.store.serializer;

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
    private int remainLength;
    /**
     * length of message
     */
    private int messageLength;
    /**
     * wrotePosition, commitLogOffset of mapped file
     */
    private long logOffset;

    public SerializerChainContext(List<Serializer> serializers) {
        super(serializers);
        this.messageLength = 0;
    }

    public int calcMessageLength() {
        for (Serializer serializer : this.serializers) {
            serializer.preSerializer(this);
        }
        return this.messageLength;
    }

    public void next() throws SerializeException {
        if (this.index >= this.serializers.size()) {
            this.index = 0;
            return;
        }
        Serializer serializer = this.serializers.get(index++);
        serializer.serialize(this);
    }

    public void addMessageLength(int length) {
        this.messageLength += length;
    }
}
