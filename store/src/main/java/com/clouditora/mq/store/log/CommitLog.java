package com.clouditora.mq.store.log;

import com.clouditora.mq.common.message.MessageEntity;
import com.clouditora.mq.store.MessageStoreConfig;
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
import java.util.concurrent.locks.ReentrantLock;

/**
 * @link org.apache.rocketmq.store.CommitLog
 */
@Slf4j
public class CommitLog {
    @Getter
    private final int fileSize;
    private final MappedFileQueue<MappedFile> commitLogQueue;
    private final ThreadLocal<ByteBufferSerializer> tlSerializer;
    private final ReentrantLock lock = new ReentrantLock();

    public CommitLog(MessageStoreConfig config) {
        this.fileSize = config.getCommitLogFileSize();
        this.commitLogQueue = new MappedFileQueue<>(config.getCommitLogPath(), config.getCommitLogFileSize());
        this.tlSerializer = ThreadLocal.withInitial(ByteBufferSerializer::new);
    }

    /**
     * org.apache.rocketmq.store.CommitLog#asyncPutMessage
     */
    public CompletableFuture<PutResult> asyncPut(MessageEntity message) {
        MappedFile commitLog = this.commitLogQueue.getOrCreate();
        if (commitLog == null) {
            log.error("create commit log error: topic={}, bornHost={}", message.getTopic(), message.getBornHost());
            return PutResult.buildAsync(PutStatus.CREATE_MAPPED_FILE_FAILED);
        }
        try {
            this.lock.lock();
            long queueOffset = 0L;
            message.setQueueOffset(queueOffset);
            // 当前文件写指针
            int writePosition = commitLog.getWritePosition();
            // 物理偏移量
            long physicalOffset = commitLog.getStartOffset() + writePosition;
            // 当前文件剩余空间
            int remainLength = commitLog.getFileSize() - writePosition;
            ByteBuffer byteBuffer = this.tlSerializer.get().serialize(physicalOffset, remainLength, message);
            commitLog.append(byteBuffer);
            return PutResult.buildAsync(PutStatus.SUCCESS, message.getMessageId(), queueOffset);
        } catch (EndOfFileException e) {
            log.debug("end of file: file={}, messageLength={}", commitLog, e.getMessageLength());
            // 剩余空间不够了: 1.当前文件写满; 2.消息写入下一个文件
            fillBlank(commitLog, e);
            return asyncPut(message);
        } catch (SerializeException e) {
            return PutResult.buildAsync(PutStatus.MESSAGE_ILLEGAL);
        } catch (Exception e) {
            log.error("unknown error", e);
            return PutResult.buildAsync(PutStatus.UNKNOWN_ERROR);
        } finally {
            this.lock.unlock();
        }
    }

    private static void fillBlank(MappedFile file, EndOfFileException e) {
        try {
            file.append(e.getByteBuffer(), e.getRemainLength());
        } catch (PutException ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * @link org.apache.rocketmq.store.CommitLog#getData
     */
    public MappedFile slice(long offset, int length) {
        MappedFile file = this.commitLogQueue.slice(offset);
        if (file == null) {
            if (offset == 0) {
                return this.commitLogQueue.getFirstFile();
            }
            return null;
        }
        int position = (int) (offset % this.fileSize);
        return file.slice(position, length);
    }

    /**
     * @link org.apache.rocketmq.store.CommitLog#getData
     */
    public MappedFile slice(long offset) {
        return slice(offset, 0);
    }
}
