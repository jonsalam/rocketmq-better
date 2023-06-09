package com.clouditora.mq.store.log;

import com.clouditora.mq.common.message.MessageEntity;
import com.clouditora.mq.store.StoreConfig;
import com.clouditora.mq.store.StoreController;
import com.clouditora.mq.store.enums.PutMessageStatus;
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
import java.nio.MappedByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @link org.apache.rocketmq.store.CommitLog
 */
@Slf4j
public class CommitLog implements File {
    private final StoreController storeController;
    @Getter
    private final int fileSize;
    private final MappedFileQueue<MappedFile> fileQueue;
    private final ByteBufferSerializer serializer;
    private final ByteBufferDeserializer deserializer;
    /**
     * @link org.apache.rocketmq.store.CommitLog#putMessageLock
     */
    private final ReentrantLock lock = new ReentrantLock();

    public CommitLog(StoreConfig config, StoreController storeController) {
        this.fileSize = config.getCommitLogFileSize();
        this.fileQueue = new MappedFileQueue<>(config.getCommitLogPath(), config.getCommitLogFileSize());
        this.storeController = storeController;
        this.serializer = new ByteBufferSerializer();
        this.deserializer = new ByteBufferDeserializer();
    }

    /**
     * @link org.apache.rocketmq.store.CommitLog#recoverNormally
     */
    @Override
    public void map() {
        this.fileQueue.map();
        List<MappedFile> files = this.fileQueue.getFiles();
        if (CollectionUtils.isEmpty(files)) {
            this.fileQueue.setFlushOffset(0);
            // TODO 删除索引文件
            return;
        }
        MappedFile file = files.get(Math.max(files.size() - 3, 0));
        long offset = file.getOffset();
        CommitLogIterator iterator = new CommitLogIterator(this, offset);
        while (iterator.hasNext()) {
            MessageEntity message = iterator.next();
            if (message == null) {
                break;
            }
            offset += message.getMessageLength();
        }
        afterMap(offset);
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
    public long flush(int pages) {
        return this.fileQueue.flush(pages);
    }

    public long getFlushOffset() {
        return this.fileQueue.getFlushOffset();
    }

    /**
     * @link org.apache.rocketmq.store.CommitLog#getData
     */
    public MappedFile slice(long offset, int length) {
        MappedFile file = this.fileQueue.slice(offset);
        if (file == null) {
            if (length == 0) {
                return this.fileQueue.getFirst();
            }
            return null;
        }
        int position = (int) (offset % this.fileSize);
        return file.slice(position, length);
    }

    /**
     * @link org.apache.rocketmq.store.CommitLog#recoverAbnormally
     */
    public void recover() {
        this.fileQueue.map();
        List<MappedFile> files = this.fileQueue.getFiles();
        if (CollectionUtils.isEmpty(files)) {
            this.fileQueue.setFlushOffset(0);
            // TODO 删除索引文件
            return;
        }
        long offset = 0;
        for (int index = Math.max(files.size() - 1, 0); index >= 0; index--) {
            MappedFile file = files.get(index);
            offset = file.getOffset();
            MappedByteBuffer byteBuffer = file.getByteBuffer();
            MessageEntity message = deserializer.deserialize(byteBuffer);
            if (message == null) {
                break;
            }
            offset += message.getMessageLength();
            this.storeController.dispatch(message);
        }
        afterMap(offset);
    }

    /**
     * @link org.apache.rocketmq.store.MappedFileQueue#truncateDirtyFiles
     */
    private void afterMap(long offset) {
        this.fileQueue.setFlushOffset(offset);
        // 修正偏移量
        MappedFile file = this.fileQueue.slice(offset);
        file.setWritePosition((int) (offset % file.getFileSize()));
        file.setFlushPosition((int) (offset % file.getFileSize()));
        // 删除多余文件
        this.fileQueue.delete(offset);
        // TODO Clear ConsumeQueue redundant data
    }

    /**
     * org.apache.rocketmq.store.CommitLog#asyncPutMessage
     */
    public CompletableFuture<PutResult> put(MessageEntity message) {
        MappedFile file = this.fileQueue.getOrCreate();
        if (file == null) {
            log.error("create commit log error: topic={}, bornHost={}", message.getTopic(), message.getBornHost());
            return PutResult.buildAsync(PutMessageStatus.CREATE_MAPPED_FILE_FAILED);
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
            return PutResult.buildAsync(PutMessageStatus.SUCCESS, message.getMessageId(), queueOffset);
        } catch (EndOfFileException e) {
            log.debug("end of file: file={}, messageLength={}", file, e.getLength());
            // 剩余空间不够了: 1.当前文件写满; 2.消息写入下一个文件
            file.fillBlank(e.getByteBuffer(), e.getFree());
            return put(message);
        } catch (SerializeException e) {
            return PutResult.buildAsync(PutMessageStatus.MESSAGE_ILLEGAL);
        } catch (Exception e) {
            log.error("unknown error", e);
            return PutResult.buildAsync(PutMessageStatus.UNKNOWN_ERROR);
        } finally {
            this.lock.unlock();
        }
    }

}
