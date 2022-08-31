package com.clouditora.mq.store.serializer;

import com.clouditora.mq.store.serializer.serializer.*;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractSerializer<T extends AbstractChain> {
    protected final T chain;

    public AbstractSerializer() {
        List<Serializer> serializers = new ArrayList<>();
        serializers.add(new MessageLengthSerializer());
        serializers.add(new MagicCodeSerializer());
        serializers.add(new BodyCrcSerializer());
        serializers.add(new QueueIdSerializer());
        serializers.add(new FlagSerializer());
        serializers.add(new QueueOffsetSerializer());
        serializers.add(new LogOffsetSerializer());
        serializers.add(new SysFlagSerializer());
        serializers.add(new BornTimestampSerializer());
        serializers.add(new BornHostSerializer());
        serializers.add(new StoreTimestampSerializer());
        serializers.add(new StoreHostSerializer());
        serializers.add(new ReConsumeTimesSerializer());
        serializers.add(new TransactionOffsetSerializer());
        serializers.add(new BodySerializer());
        serializers.add(new TopicSerializer());
        serializers.add(new PropertiesSerializer());
        serializers.add(new MessageIdSerializer());

        this.chain = init(serializers);
    }

    protected abstract T init(List<Serializer> serializers);
}
