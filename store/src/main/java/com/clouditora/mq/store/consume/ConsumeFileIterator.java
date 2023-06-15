package com.clouditora.mq.store.consume;

import com.clouditora.mq.store.file.MappedFile;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.Iterator;

@Slf4j
public class ConsumeFileIterator implements Iterator<ConsumeFileEntity> {
    private final ConsumeQueue consumeQueue;
    @Getter
    private long offset;
    private MappedFile file;
    private ByteBuffer byteBuffer;
    private long position = 0;

    private boolean prevBlankMessage = false;
    private boolean hasNext = true;

    public ConsumeFileIterator(ConsumeQueue consumeQueue, long offset) {
        this.consumeQueue = consumeQueue;
        this.file = consumeQueue.slice(offset);
        if (this.file == null) {
            this.hasNext = false;
            return;
        }
        this.offset = this.file.getOffset();
        this.byteBuffer = this.file.getByteBuffer();
    }

    @Override
    public boolean hasNext() {
        if (this.file == null) {
            return false;
        }
        if (this.position > this.file.getWritePosition()) {
            return false;
        }
        return hasNext;
    }

    @Override
    public ConsumeFileEntity next() {
        if (!hasNext()) {
            return null;
        }
        ConsumeFileEntity entity = deserialize(byteBuffer);
        if (entity != null) {
            this.prevBlankMessage = false;
            this.offset += entity.getMessageLength();
            this.position += entity.getMessageLength();
        } else {
            // 空白消息
            if (this.prevBlankMessage) {
                // 连续2个空白消息
                this.hasNext = false;
                return null;
            }
            this.prevBlankMessage = true;
            this.file.release();
            // 下一个文件的offset
            this.offset = this.offset + this.file.getFileSize() - this.offset % this.file.getFileSize();
            this.file = this.consumeQueue.slice(this.offset);
            if (this.file == null) {
                // 下一个文件没有了
                return null;
            }
            this.byteBuffer = this.file.getByteBuffer();
            // 读取下一个文件的消息
            return next();
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
