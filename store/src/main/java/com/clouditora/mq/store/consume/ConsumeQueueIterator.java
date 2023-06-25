package com.clouditora.mq.store.consume;

import com.clouditora.mq.store.file.MappedFile;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.Iterator;

@Slf4j
public class ConsumeQueueIterator implements Iterator<ConsumeFileEntity> {
    private final ConsumeQueue consumeQueue;
    private MappedFile file;
    private ByteBuffer byteBuffer;
    private long offset = 0;
    private boolean hasNext = true;

    public ConsumeQueueIterator(ConsumeQueue consumeQueue, long offset) {
        this.consumeQueue = consumeQueue;
        this.file = consumeQueue.slice(offset);
        if (this.file == null) {
            this.hasNext = false;
            return;
        }
        this.byteBuffer = this.file.getByteBuffer();
    }

    @Override
    public boolean hasNext() {
        if (this.file == null) {
            return false;
        }
        if (this.offset > this.file.getWritePosition()) {
            return false;
        }
        return hasNext;
    }

    @Override
    public ConsumeFileEntity next() {
        if (this.offset >= this.file.getWritePosition()) {
            // 下一个文件的offset
            this.file = this.consumeQueue.slice(this.offset);
            if (this.file == null) {
                // 下一个文件没有了
                return null;
            }
            this.byteBuffer = this.file.getByteBuffer();
            this.offset = 0;
            // 读取下一个文件的消息
            return next();
        }
        ConsumeFileEntity entity = deserialize(byteBuffer);
        if (entity != null) {
            this.offset += ConsumeFile.UNIT_SIZE;
        }
        return entity;
    }

    public ConsumeFileEntity deserialize(ByteBuffer byteBuffer) {
        long commitLogOffset = byteBuffer.getLong();
        int messageLength = byteBuffer.getInt();
        long tagsCode = byteBuffer.getLong();
        if (commitLogOffset < 0 || messageLength <= 0) {
            log.info("deserialize consume queue end: {}@{}", this.file.getFile(), byteBuffer.position());
            return null;
        }

        ConsumeFileEntity entity = new ConsumeFileEntity();
        entity.setCommitLogOffset(commitLogOffset);
        entity.setMessageLength(messageLength);
        entity.setTagsCode(tagsCode);
        return entity;
    }
}
