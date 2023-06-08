package com.clouditora.mq.store.file;

import com.clouditora.mq.store.exception.PutException;
import com.clouditora.mq.store.util.StoreUtil;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class MappedFileQueueTest extends AbstractFileTest {

    @Test
    void getOrCreate_0() {
        MappedFileQueue<MappedFile> queue = new MappedFileQueue<>(super.path, 180);
        MappedFile file = queue.getOrCreate();
        assertThat(file.file.getName()).isEqualTo(StoreUtil.long2String(0));

        file.writePosition.set(180);
        file = queue.getOrCreate();
        assertThat(file.file.getName()).isEqualTo(StoreUtil.long2String(180));
    }

    @Test
    void getOrCreate_1() {
        MappedFileQueue<MappedFile> queue = new MappedFileQueue<>(super.path, 180);
        MappedFile file = queue.getOrCreate(10);
        assertThat(file.file.getName()).isEqualTo(StoreUtil.long2String(0));

        file.writePosition.set(180);
        file = queue.getOrCreate(190);
        assertThat(file.file.getName()).isEqualTo(StoreUtil.long2String(180));
    }

    @Test
    void getOrCreate_2() {
        MappedFileQueue<MappedFile> queue = new MappedFileQueue<>(super.path, 180);
        MappedFile file = queue.getOrCreate(190);
        assertThat(file.file.getName()).isEqualTo(StoreUtil.long2String(180));
    }

    @Test
    void slice_0() {
        MappedFileQueue<MappedFile> queue = new MappedFileQueue<>(super.path, 180);
        MappedFile file = queue.slice(180);
        assertThat(file).isNull();
    }

    @Test
    void slice_1() {
        MappedFileQueue<MappedFile> queue = new MappedFileQueue<>(super.path, 180);
        queue.getOrCreate(0);

        MappedFile file = queue.slice(180);
        assertThat(file).isNull();
    }

    @Test
    void slice_2() {
        MappedFileQueue<MappedFile> queue = new MappedFileQueue<>(super.path, 180);
        queue.getOrCreate(0);

        MappedFile file = queue.slice(10);
        assertThat(file.file.getName()).isEqualTo(StoreUtil.long2String(0));
    }

    @Test
    void slice_3() {
        MappedFileQueue<MappedFile> queue = new MappedFileQueue<>(super.path, 180);
        MappedFile file = queue.getOrCreate(0);
        file.writePosition.set(180);
        queue.getOrCreate(180);

        file = queue.slice(190);
        assertThat(file.file.getName()).isEqualTo(StoreUtil.long2String(180));

        file = queue.slice(360);
        assertThat(file).isNull();
    }

    @Test
    void testFindMappedFileByOffset() throws PutException {
        // four-byte string.
        final String fixedMsg = "abcd";

        MappedFileQueue<MappedFile> mappedFileQueue = new MappedFileQueue<>(super.path, 1024);
        for (int i = 0; i < 1024; i++) {
            MappedFile mappedFile = mappedFileQueue.getOrCreate();
            assertThat(mappedFile).isNotNull();
            mappedFile.append(fixedMsg.getBytes());
        }

        assertThat(mappedFileQueue.getMappedMemorySize()).isEqualTo(fixedMsg.getBytes().length * 1024L);

        MappedFile mappedFile = mappedFileQueue.slice(0);
        assertThat(mappedFile).isNotNull();
        assertThat(mappedFile.getOffset()).isEqualTo(0);

        mappedFile = mappedFileQueue.slice(100);
        assertThat(mappedFile).isNotNull();
        assertThat(mappedFile.getOffset()).isEqualTo(0);

        mappedFile = mappedFileQueue.slice(1024);
        assertThat(mappedFile).isNotNull();
        assertThat(mappedFile.getOffset()).isEqualTo(1024);

        mappedFile = mappedFileQueue.slice(1024 + 100);
        assertThat(mappedFile).isNotNull();
        assertThat(mappedFile.getOffset()).isEqualTo(1024);

        mappedFile = mappedFileQueue.slice(1024 * 2);
        assertThat(mappedFile).isNotNull();
        assertThat(mappedFile.getOffset()).isEqualTo(1024 * 2);

        mappedFile = mappedFileQueue.slice(1024 * 2 + 100);
        assertThat(mappedFile).isNotNull();
        assertThat(mappedFile.getOffset()).isEqualTo(1024 * 2);

        // over mapped memory size.
        mappedFile = mappedFileQueue.slice(1024 * 4);
        assertThat(mappedFile).isNull();

        mappedFile = mappedFileQueue.slice(1024 * 4 + 100);
        assertThat(mappedFile).isNull();

        mappedFileQueue.unmap();
        mappedFileQueue.delete();
    }

    @Test
    void testFindMappedFileByOffset_StartOffsetIsNonZero() {
        MappedFileQueue<MappedFile> mappedFileQueue = new MappedFileQueue<>(super.path, 1024);

        //Start from a non-zero offset
        MappedFile mappedFile = mappedFileQueue.getOrCreate(1024);
        assertThat(mappedFile).isNotNull();

        assertThat(mappedFileQueue.slice(1025)).isEqualTo(mappedFile);

        assertThat(mappedFileQueue.slice(0)).isNull();
        assertThat(mappedFileQueue.slice(123)).isNull();

        assertThat(mappedFileQueue.slice(0)).isNull();

        mappedFileQueue.unmap();
        mappedFileQueue.delete();
    }

    @Test
    void testAppendMessage() throws PutException {
        final String fixedMsg = "0123456789abcdef";

        MappedFileQueue<MappedFile> mappedFileQueue = new MappedFileQueue<>(super.path, 1024);

        for (int i = 0; i < 1024; i++) {
            MappedFile mappedFile = mappedFileQueue.getOrCreate(0);
            assertThat(mappedFile).isNotNull();
            mappedFile.append(fixedMsg.getBytes());
        }

        mappedFileQueue.flush(0);
        assertThat(mappedFileQueue.getFlushOffset()).isEqualTo(1024);

        mappedFileQueue.flush(0);
        assertThat(mappedFileQueue.getFlushOffset()).isEqualTo(1024 * 2);

        mappedFileQueue.flush(0);
        assertThat(mappedFileQueue.getFlushOffset()).isEqualTo(1024 * 3);

        mappedFileQueue.flush(0);
        assertThat(mappedFileQueue.getFlushOffset()).isEqualTo(1024 * 4);

        mappedFileQueue.flush(0);
        assertThat(mappedFileQueue.getFlushOffset()).isEqualTo(1024 * 5);

        mappedFileQueue.flush(0);
        assertThat(mappedFileQueue.getFlushOffset()).isEqualTo(1024 * 6);

        mappedFileQueue.unmap();
        mappedFileQueue.delete();
    }

    @Test
    void testGetMappedMemorySize() throws PutException {
        final String fixedMsg = "abcd";

        MappedFileQueue<MappedFile> mappedFileQueue = new MappedFileQueue<>(super.path, 1024);

        for (int i = 0; i < 1024; i++) {
            MappedFile mappedFile = mappedFileQueue.getOrCreate(0);
            assertThat(mappedFile).isNotNull();
            mappedFile.append(fixedMsg.getBytes());
        }

        assertThat(mappedFileQueue.getMappedMemorySize()).isEqualTo(fixedMsg.length() * 1024);
        mappedFileQueue.unmap();
        mappedFileQueue.delete();
    }
}
