package com.clouditora.mq.store.log;

import com.clouditora.mq.common.constant.MagicCode;
import com.clouditora.mq.common.message.MessageEntity;
import com.clouditora.mq.store.MessageStoreConfig;
import com.clouditora.mq.store.enums.PutStatus;
import com.clouditora.mq.store.exception.PutException;
import com.clouditora.mq.store.file.File;
import com.clouditora.mq.store.file.MappedFile;
import com.clouditora.mq.store.file.MappedFileQueue;
import com.clouditora.mq.store.file.PutResult;
import com.clouditora.mq.store.serialize.ByteBufferDeserializer;
import com.clouditora.mq.store.serialize.ByteBufferSerializer;
import com.clouditora.mq.store.serialize.EndOfFileException;
import com.clouditora.mq.store.serialize.SerializeException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @link org.apache.rocketmq.store.CommitLog
 */
@Slf4j
public class CommitLog implements File {
    @Getter
    private final int fileSize;
    private final MappedFileQueue<MappedFile> fileQueue;
    private final ByteBufferSerializer serializer;
    private final ByteBufferDeserializer deserializer;
    /**
     * @link org.apache.rocketmq.store.CommitLog#putMessageLock
     */
    private final ReentrantLock lock = new ReentrantLock();

    public CommitLog(MessageStoreConfig config) {
        this.fileSize = config.getCommitLogFileSize();
        this.fileQueue = new MappedFileQueue<>(config.getCommitLogPath(), config.getCommitLogFileSize());
        this.serializer = new ByteBufferSerializer();
        this.deserializer = new ByteBufferDeserializer();
    }

    @Override
    public void map() {
        this.fileQueue.map();
    }

    @Override
    public void unmap() {
        this.fileQueue.unmap();
    }

    @Override
    public void delete() {
        this.fileQueue.delete();
    }

    @Override
    public void flush(int pages) {
        this.fileQueue.flush(pages);
    }

    /**
     * @link org.apache.rocketmq.store.CommitLog#flush
     */
    public void flush() {
        this.fileQueue.flush(0);
    }

    /**
     * @link org.apache.rocketmq.store.CommitLog#getData
     */
    public MappedFile slice(long offset, int length) {
        MappedFile file = this.fileQueue.slice(offset);
        if (file == null) {
            if (offset == 0) {
                return this.fileQueue.getFirstFile();
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

    /**
     * @link org.apache.rocketmq.store.CommitLog#recoverNormally
     */
    public void recover() {
        map();
        List<MappedFile> files = this.fileQueue.getFiles();
        if (CollectionUtils.isEmpty(files)) {
            return;
        }
        MappedFile file = files.get(Math.max(files.size() - 3, 0));
        long offset = file.getOffset();
        CommitLogIterator iterator = new CommitLogIterator(this, offset);
        while (iterator.hasNext()) {
            MessageEntity message = iterator.next();
            if (message == null || message.getMagicCode() == MagicCode.ERROR) {
                log.error("found illegal message: file={}, position={}", file, iterator.getOffset());
                break;
            }
            offset += message.getMessageLength();
        }
        this.fileQueue.setFlushOffset(offset);
        // 修正偏移量
        file = this.fileQueue.slice(offset);
        file.setWritePosition((int) (offset % file.getFileSize()));
        file.setFlushPosition((int) (offset % file.getFileSize()));
        file.release();
        // 删除多余文件
        this.fileQueue.delete(offset);
    }

    /**
     * org.apache.rocketmq.store.CommitLog#asyncPutMessage
     */
    public CompletableFuture<PutResult> put(MessageEntity message) {
            MappedFile file = this.fileQueue.getOrCreate();
        if (file == null) {
            log.error("create commit log error: topic={}, bornHost={}", message.getTopic(), message.getBornHost());
            return PutResult.buildAsync(PutStatus.CREATE_MAPPED_FILE_FAILED);
        }
        try {
            this.lock.lock();
            long queueOffset = 0L;
            message.setQueueOffset(queueOffset);
            // 当前文件写指针
            int writePosition = file.getWritePosition();
            // 物理偏移量
            long offset = file.getOffset() + writePosition;
            // 当前文件剩余空间
            int free = file.getFileSize() - writePosition;
            ByteBuffer byteBuffer = this.serializer.serialize(offset, free, message);
            file.append(byteBuffer);
            return PutResult.buildAsync(PutStatus.SUCCESS, message.getMessageId(), queueOffset);
        } catch (EndOfFileException e) {
            log.debug("end of file: file={}, messageLength={}", file, e.getLength());
            // 剩余空间不够了: 1.当前文件写满; 2.消息写入下一个文件
            fillBlank(file, e.getByteBuffer(), e.getFree());
            return put(message);
        } catch (SerializeException e) {
            return PutResult.buildAsync(PutStatus.MESSAGE_ILLEGAL);
        } catch (Exception e) {
            log.error("unknown error", e);
            return PutResult.buildAsync(PutStatus.UNKNOWN_ERROR);
        } finally {
            this.lock.unlock();
        }
    }

    private static void fillBlank(MappedFile file, ByteBuffer byteBuffer, int length) {
        try {
            file.fillBlank(byteBuffer, length);
        } catch (PutException ex) {
            throw new IllegalStateException(ex);
        }
    }
}
