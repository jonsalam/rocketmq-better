package com.clouditora.mq.store.log;

import com.clouditora.mq.common.constant.MagicCode;
import com.clouditora.mq.common.message.MessageEntity;
import com.clouditora.mq.store.file.MappedFile;
import com.clouditora.mq.store.serialize.ByteBufferDeserializer;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;

@Slf4j
public class CommitLogReader {
    private final ThreadLocal<ByteBufferDeserializer> deserializer = ThreadLocal.withInitial(ByteBufferDeserializer::new);
    private final CommitLog commitLog;
    @Getter
    @Setter
    private long offset;
    private MappedFile file;
    private ByteBuffer byteBuffer;
    private long position = 0;

    private boolean prevBlankMessage = false;
    private boolean hasNext = true;

    public CommitLogReader(CommitLog commitLog, long offset) {
        this.commitLog = commitLog;
        this.file = commitLog.slice(offset, -1);
        if (this.file == null) {
            this.hasNext = false;
            return;
        }
        this.offset = this.file.getOffset();
        this.byteBuffer = this.file.getByteBuffer();
    }

    public boolean hasNext() {
        if (this.file == null) {
            return false;
        }
        if (this.position > this.file.getWritePosition()) {
            return false;
        }
        return hasNext;
    }

    public MessageEntity read() {
        if (!hasNext()) {
            return null;
        }
        MessageEntity entity;
        try {
            entity = this.deserializer.get().deserialize(this.byteBuffer);
        } catch (Exception e) {
            log.error("反序列化CommitLog失败: file={}@{}", this.file.getFile(), this.position, e);
            this.hasNext = false;
            return null;
        }
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
            // 下一个文件的offset
            this.offset = this.offset + this.file.getFileSize() - this.offset % this.file.getFileSize();
            this.file.release();
            this.file = this.commitLog.slice(this.offset, -1);
            if (this.file == null) {
                // 下一个文件没有了
                return null;
            }
            this.byteBuffer = this.file.getByteBuffer();
            this.position = 0;
            // 读取下一个文件的消息
            return read();
        }
        return entity;
    }
}
