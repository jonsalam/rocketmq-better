package com.clouditora.mq.store.serializer;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@EqualsAndHashCode(callSuper = true)
@Setter
@Getter
public class SerializerChain extends AbstractChain {
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

    public SerializerChain(List<Serializer> serializers) {
        super(serializers);
        this.messageLength = 0;
    }

    public int calcMessageLength() {
        for (Serializer serializer : serializers) {
            serializer.preSerializer(this);
        }
        return messageLength;
    }

    public void next() throws SerializeException {
        if (index >= serializers.size()) {
            index = 0;
            return;
        }
        Serializer serializer = serializers.get(index++);
        serializer.serialize(this);
    }

    public void addMessageLength(int length) {
        messageLength += length;
    }
}
