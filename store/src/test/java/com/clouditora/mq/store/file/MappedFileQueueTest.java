package com.clouditora.mq.store.file;

import com.clouditora.mq.store.AbstractFileTest;
import com.clouditora.mq.store.util.StoreUtil;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class MappedFileQueueTest extends AbstractFileTest {

    @Test
    void getOrCreate_0() {
        MappedFileQueue<MappedFile> queue = new MappedFileQueue<>(super.path, 180);
        MappedFile file = queue.getOrCreate();
        assertEquals(StoreUtil.long2String(0), file.file.getName());

        file.writePosition.set(180);
        file = queue.getOrCreate();
        assertEquals(StoreUtil.long2String(180), file.file.getName());
    }

    @Test
    void getOrCreate_1() {
        MappedFileQueue<MappedFile> queue = new MappedFileQueue<>(super.path, 180);
        MappedFile file = queue.getOrCreate(10);
        assertEquals(StoreUtil.long2String(0), file.file.getName());

        file.writePosition.set(180);
        file = queue.getOrCreate(190);
        assertEquals(StoreUtil.long2String(180), file.file.getName());
    }

    @Test
    void getOrCreate_2() {
        MappedFileQueue<MappedFile> queue = new MappedFileQueue<>(super.path, 180);
        MappedFile file = queue.getOrCreate(190);
        assertEquals(StoreUtil.long2String(180), file.file.getName());
    }

    @Test
    public void slice_0() {
        MappedFileQueue<MappedFile> queue = new MappedFileQueue<>(super.path, 180);
        MappedFile file = queue.slice(180);
        assertNull(file);
    }

    @Test
    public void slice_1() {
        MappedFileQueue<MappedFile> queue = new MappedFileQueue<>(super.path, 180);
        queue.getOrCreate(0);

        MappedFile file = queue.slice(180);
        assertNull(file);
    }

    @Test
    public void slice_2() {
        MappedFileQueue<MappedFile> queue = new MappedFileQueue<>(super.path, 180);
        queue.getOrCreate(0);

        MappedFile file = queue.slice(10);
        assertEquals(StoreUtil.long2String(0), file.file.getName());
    }

    @Test
    public void slice_3() {
        MappedFileQueue<MappedFile> queue = new MappedFileQueue<>(super.path, 180);
        MappedFile file = queue.getOrCreate(0);
        file.writePosition.set(180);
        queue.getOrCreate(180);

        file = queue.slice(190);
        assertEquals(StoreUtil.long2String(180), file.file.getName());

        file = queue.slice(1024);
        assertNull(file);
    }
}
