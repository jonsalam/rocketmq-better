package com.clouditora.mq.store.file;

import com.clouditora.mq.store.AbstractFileTest;
import com.clouditora.mq.store.exception.PutException;
import com.clouditora.mq.store.util.StoreUtil;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

public class MappedFileQueueTest extends AbstractFileTest {

    @Test
    public void getCurrentWritingFile() throws PutException {
        MappedFileQueue<MappedFile> mappedFileQueue = new MappedFileQueue<>(path, 180);
        MappedFile mappedFile1 = mappedFileQueue.getOrCreate();
        mappedFile1.append(ByteBuffer.allocate(180));
        assertTrue(mappedFile1.isFull());

        MappedFile mappedFile2 = mappedFileQueue.getOrCreate();
        assertNotEquals(mappedFile1, mappedFile2);
        assertEquals(mappedFile2.getFile().getName(), StoreUtil.long2String(180));
    }
}
