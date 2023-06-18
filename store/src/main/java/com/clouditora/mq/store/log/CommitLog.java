package com.clouditora.mq.store.log;

import com.clouditora.mq.common.message.MessageEntity;
import com.clouditora.mq.store.StoreConfig;
import com.clouditora.mq.store.StoreController;
import com.clouditora.mq.store.file.*;
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
        // @link org.apache.rocketmq.store.CommitLog#load
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
    public void recover(boolean normally) {
        List<MappedFile> files = this.fileQueue.getFiles();
        if (CollectionUtils.isEmpty(files)) {
            return;
        }
        // 只处理最后3个文件
        MappedFile file;
        if (normally) {
            file = files.get(Math.max(files.size() - 3, 0));
        } else {
            // 遍历所有文件, 有了checkpoint可以过滤有效的文件
            file = this.fileQueue.getFirst();
        }
        long offset = file.getOffset();
        CommitLogReader iterator = new CommitLogReader(this, offset);
        MessageEntity entity;
        while ((entity = iterator.read()) != null) {
            offset = entity.getCommitLogOffset() + entity.getMessageLength();
            if (!normally) {
                // 重建索引
                this.storeController.dispatch(entity);
            }
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
        // TODO Clear ConsumeQueueManager redundant data
    }

    /**
     * @link org.apache.rocketmq.store.CommitLog#getMinOffset
     */
    public long getMinOffset() {
        MappedFile file = this.fileQueue.getFirst();
        if (file == null) {
            return -1;
        }
        return file.getOffset();
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
            file.fillBlank(e.getByteBuffer(), e.getFree());
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
}
