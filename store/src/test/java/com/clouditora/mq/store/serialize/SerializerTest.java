package com.clouditora.mq.store.serialize;

import com.clouditora.mq.common.message.MessageEntity;
import com.clouditora.mq.store.TestUtil;
import com.clouditora.mq.store.exception.PutException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(MockitoExtension.class)
public class SerializerTest {

    @Test
    public void serializeAndDeserialize() throws PutException {
        MessageEntity message1 = TestUtil.buildMessage();
        ByteBufferSerializer serializer = new ByteBufferSerializer();
        ByteBuffer buffer = serializer.serialize(0, 1024 * 10, message1);

        ByteBufferDeserializer deserializer = new ByteBufferDeserializer();
        MessageEntity message2 = deserializer.deserialize(buffer);
        assertEquals(message1, message2);
    }
}
