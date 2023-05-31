package com.clouditora.mq.store.file;

import com.clouditora.mq.common.message.MessageEntity;
import com.clouditora.mq.store.AbstractFileTest;
import com.clouditora.mq.store.TestUtil;
import com.clouditora.mq.store.serializer.ByteBufferDeserializer;
import com.clouditora.mq.store.serializer.ByteBufferSerializer;
import com.clouditora.mq.store.util.StoreUtil;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MappedFileTest extends AbstractFileTest {

    public MessageEntity write(String path) throws Exception {
        MappedFile mappedFile = new MappedFile(path, 1024 * 100);
        MessageEntity message = TestUtil.buildMessage();
        ByteBuffer byteBuffer = new ByteBufferSerializer().serialize(0, 1024 * 100, message);
        mappedFile.append(byteBuffer);
        return message;
    }

    public MessageEntity read(String path) throws Exception {
        MappedFile mappedFile = new MappedFile(path, 1024 * 100);
        ByteBuffer byteBuffer = mappedFile.getByteBuffer();
        return new ByteBufferDeserializer().deserialize(byteBuffer);
    }

    @Test
    public void writeAndRead() throws Exception {
        String path = super.path + StoreUtil.long2String(0);
        MessageEntity write = write(path);
        MessageEntity read = read(path);
        assertEquals(write, read);
    }

}
