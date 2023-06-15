package com.clouditora.mq.store.log;

import com.clouditora.mq.common.constant.MagicCode;
import com.clouditora.mq.common.message.MessageEntity;
import com.clouditora.mq.store.file.MappedFile;
import com.clouditora.mq.store.serialize.ByteBufferDeserializer;
import lombok.Getter;

import java.nio.ByteBuffer;
import java.util.Iterator;

public class CommitLogIterator implements Iterator<MessageEntity> {
    private final ThreadLocal<ByteBufferDeserializer> deserializer = ThreadLocal.withInitial(ByteBufferDeserializer::new);
    private final CommitLog commitLog;
    @Getter
    private long offset;
    private MappedFile file;
    private ByteBuffer byteBuffer;
    private long position = 0;

    private boolean prevBlankMessage = false;
    private boolean hasNext = true;

    public CommitLogIterator(CommitLog commitLog, long offset) {
        this.commitLog = commitLog;
        this.file = commitLog.slice(offset, 0);
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
    public MessageEntity next() {
        MessageEntity entity = this.deserializer.get().deserialize(this.byteBuffer);
        if (entity.getMagicCode() == MagicCode.MESSAGE) {
            this.prevBlankMessage = false;
            this.offset += entity.getMessageLength();
            this.position += entity.getMessageLength();
        } else if (entity.getMagicCode() == MagicCode.BLANK) {
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
            this.file = this.commitLog.slice(this.offset, 0);
            if (this.file == null) {
                // 下一个文件没有了
                return null;
            }
            this.byteBuffer = this.file.getByteBuffer();
            this.position = 0;
            // 读取下一个文件的消息
            return next();
        }
        return entity;
    }
}
