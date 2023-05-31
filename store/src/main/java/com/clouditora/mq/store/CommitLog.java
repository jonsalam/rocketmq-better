package com.clouditora.mq.store;

import com.clouditora.mq.common.message.MessageEntity;
import com.clouditora.mq.store.enums.PutStatus;
import com.clouditora.mq.store.exception.PutException;
import com.clouditora.mq.store.file.MappedFile;
import com.clouditora.mq.store.file.MappedFileQueue;
import com.clouditora.mq.store.file.PutResult;
import com.clouditora.mq.store.serializer.ByteBufferSerializer;
import com.clouditora.mq.store.serializer.SerializeException;
import com.clouditora.mq.store.serializer.serializer.EndOfFileException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

/**
 * @link org.apache.rocketmq.store.CommitLog
 */
@Slf4j
public class CommitLog {
    @Getter
    private final int fileSize;
    @Getter
    private final MappedFileQueue<MappedFile> commitLogQueue;
    private final ThreadLocal<ByteBufferSerializer> tlSerializer;

    public CommitLog(MessageStoreConfig config) {
        this.fileSize = config.getCommitLogFileSize();
        this.commitLogQueue = new MappedFileQueue<>(config.getCommitLogPath(), config.getCommitLogFileSize());
        this.tlSerializer = ThreadLocal.withInitial(ByteBufferSerializer::new);
    }

    /**
     * org.apache.rocketmq.store.CommitLog#asyncPutMessage
     */
    public CompletableFuture<PutResult> asyncPut(MessageEntity message) {
        MappedFile file = this.commitLogQueue.getCurrentWritingFile();
        if (file == null) {
            log.error("create file error: topic={}, bornHost={}", message.getTopic(), message.getBornHost());
            return PutResult.buildAsync(PutStatus.CREATE_MAPPED_FILE_FAILED);
        }
        try {
            Long queueOffset = 0L;
            message.setQueueOffset(queueOffset);
            // 当前文件写指针
            int writePosition = file.getWritePosition();
            // 物理偏移量
            long physicalOffset = file.getFileOffset() + writePosition;
            // 当前文件剩余空间
            int remainLength = file.getFileSize() - writePosition;
            ByteBuffer byteBuffer = this.tlSerializer.get().serialize(physicalOffset, remainLength, message);
            file.append(byteBuffer);
            return PutResult.buildAsync(PutStatus.SUCCESS, message.getMessageId(), queueOffset);
        } catch (EndOfFileException e) {
            log.debug("end of file: file={}, messageLength={}", file, e.getMessageLength());
            // 剩余空间不够, 当前文件写满, 消息写入下一个文件
            fillCurrentFile(file, e);
            return asyncPut(message);
        } catch (SerializeException e) {
            return PutResult.buildAsync(PutStatus.MESSAGE_ILLEGAL);
        } catch (Exception e) {
            log.error("unknown error", e);
        }
        return PutResult.buildAsync(PutStatus.UNKNOWN_ERROR);
    }

    private static void fillCurrentFile(MappedFile file, EndOfFileException e) {
        try {
            file.append(e.getByteBuffer(), e.getRemainLength());
        } catch (PutException ex) {
            throw new RuntimeException(ex);
        }
    }

    public MappedFile slice(long logOffset, int length) {
        MappedFile file = this.commitLogQueue.slice(logOffset);
        if (file == null) {
            return null;
        }
        int offset = (int) (logOffset % fileSize);
        return file.slice(offset, length);
    }

    public MappedFile slice(long logOffset) {
        return slice(logOffset, 0);
    }
}
