package com.clouditora.mq.store.consume;

import com.clouditora.mq.store.StoreConfig;
import com.clouditora.mq.store.file.MappedFile;
import com.clouditora.mq.store.file.MappedFileQueue;
import com.clouditora.mq.store.util.StoreUtil;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;

/**
 * @link org.apache.rocketmq.store.ConsumeQueue
 */
@Slf4j
public class ConsumeQueue extends MappedFileQueue<ConsumeFile> {
    private final StoreConfig config;
    @Getter
    private final String topic;
    @Getter
    private final int queueId;
    /**
     * @link org.apache.rocketmq.store.ConsumeQueue#maxPhysicOffset
     */
    @Getter
    @Setter
    private volatile long maxCommitLogOffset = 0;

    public ConsumeQueue(StoreConfig config, File dir) {
        super(dir, config.getConsumeQueueFileSize());
        this.config = config;
        this.topic = dir.getParent();
        this.queueId = Integer.parseInt(dir.getName());
    }

    public ConsumeQueue(StoreConfig config, String topic, int queueId) {
        super(new File("%s/%s/%s".formatted(config.getConsumeQueuePath(), topic, queueId)), config.getConsumeQueueFileSize());
        this.config = config;
        this.topic = dir.getParent();
        this.queueId = Integer.parseInt(dir.getName());
    }

    @Override
    public ConsumeFile getOrCreate(long startOffset) {
        long offset = startOffset * ConsumeFile.UNIT_SIZE;
        ConsumeFile file = super.getOrCreate(offset);
        if (file == null) {
            return null;
        }
        if (offset < file.getWritePosition() + file.getOffset()) {
            log.warn("create consume file repeatedly: expectOffset={}, currentOffset={}", offset, file.getWritePosition() + file.getOffset());
            return null;
        }
        return file;
    }

    @Override
    protected ConsumeFile create(long offset) throws IOException {
        String path = "%s/%s".formatted(super.dir, StoreUtil.long2String(offset));
        return new ConsumeFile(path, super.fileSize);
    }

    /**
     * @link org.apache.rocketmq.store.ConsumeQueueManager#getIndexBuffer
     */
    @Override
    public ConsumeFile slice(long offset) {
        return super.slice(offset);
    }

    @Override
    public long getMaxWriteOffset() {
        return super.getMaxWriteOffset() / ConsumeFile.UNIT_SIZE;
    }

    /**
     * @link org.apache.rocketmq.store.ConsumeQueueManager#recover
     */
    public void recover() {
        // 只处理最后3个文件
        MappedFile file = super.files.get(Math.max(super.files.size() - 3, 0));
        long offset = file.getOffset();
        ConsumeQueueIterator iterator = new ConsumeQueueIterator(this, offset);
        while (iterator.hasNext()) {
            ConsumeFileEntity entity = iterator.next();
            if (entity == null) {
                break;
            }
            offset += ConsumeFile.UNIT_SIZE;
            this.maxCommitLogOffset = entity.getCommitLogOffset() + entity.getMessageLength();
        }
        // 修正偏移量
        setFlushOffset(offset);
        file = slice(offset);
        file.setWritePosition((int) (offset % file.getFileSize()));
        file.setFlushPosition((int) (offset % file.getFileSize()));
    }

    /**
     * @link org.apache.rocketmq.store.ConsumeQueueManager#rollNextFile
     */
    public long rollNextFile(long offset) {
        int count = super.fileSize / ConsumeFile.UNIT_SIZE;
        return offset + count - offset % count;
    }
}
