package com.clouditora.mq.store.file;

import com.clouditora.mq.common.message.MessageEntity;
import com.clouditora.mq.store.TestUtil;
import com.clouditora.mq.store.exception.PutException;
import com.clouditora.mq.store.serialize.ByteBufferDeserializer;
import com.clouditora.mq.store.serialize.ByteBufferSerializer;
import com.clouditora.mq.store.util.StoreUtil;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MappedFileTest extends AbstractFileTest {

    MessageEntity write(String path) throws Exception {
        MappedFile file = new MappedFile(path, 1024);
        MessageEntity message = TestUtil.buildMessage();
        ByteBuffer byteBuffer = new ByteBufferSerializer().serialize(0, 1024, message);
        file.append(byteBuffer);
        return message;
    }

    MessageEntity read(String path) throws Exception {
        MappedFile file = new MappedFile(path, 1024);
        ByteBuffer byteBuffer = file.getByteBuffer();
        return new ByteBufferDeserializer().deserialize(byteBuffer);
    }

    @Test
    void writeAndRead() throws Exception {
        String path = super.path + StoreUtil.long2String(0);
        MessageEntity write = write(path);
        MessageEntity read = read(path);
        assertEquals(write, read);
    }

    @Test
    void slice() throws IOException, PutException {
        String path = super.path + StoreUtil.long2String(1024);
        MappedFile file = new MappedFile(path, 1024);
        file.append(TestUtil.MESSAGE_BODY);

        MappedFile slice = file.slice(0, 0);
        byte[] bytes = new byte[TestUtil.MESSAGE_BODY.length];
        slice.getByteBuffer().get(bytes);
        String read = new String(bytes);

        assertEquals(new String(TestUtil.MESSAGE_BODY), read);
    }
}
