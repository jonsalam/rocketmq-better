package com.clouditora.mq.store.consume;

import com.clouditora.mq.store.StoreConfig;
import com.clouditora.mq.store.file.MappedFileQueue;
import com.clouditora.mq.store.util.StoreUtil;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @link org.apache.rocketmq.store.ConsumeQueue
 */
@Slf4j
public class ConsumeQueue extends MappedFileQueue<ConsumeFile> {
    private final StoreConfig config;
    @Getter
    private volatile long minOffset = 0;
    @Getter
    private volatile long maxOffset = 0;

    public ConsumeQueue(StoreConfig config, File dir) {
        super(dir, config.getConsumeQueueFileSize());
        this.config = config;
    }

    public void increaseMaxOffset(int size) {
        this.maxOffset += size;
    }

    @Override
    protected ConsumeFile create(long offset) throws IOException {
        String path = "%s/%s".formatted(super.dir, StoreUtil.long2String(offset));
        return new ConsumeFile(path, super.fileSize);
    }

    @Override
    public ConsumeFile getOrCreate(long startOffset) {
        long offset = startOffset * ConsumeFile.UNIT_SIZE;
        ConsumeFile file = super.getOrCreate(offset);
        if (file == null) {
            return null;
        }
        if (file == super.files.get(0) && file.getWritePosition() == 0 && offset != 0) {
//            this.mappedFileQueue.setFlushedWhere(expectLogicOffset);
//            this.mappedFileQueue.setCommittedWhere(expectLogicOffset);
            this.minOffset = offset;
            fillBlank(file, offset);
        }
        if (offset < file.getWritePosition() + file.getOffset()) {
            log.warn("create consume file repeatedly: expectOffset={}, currentOffset={}", offset, file.getWritePosition() + file.getOffset());
            return null;
        }
        return file;
    }

    /**
     * @link org.apache.rocketmq.store.ConsumeQueue#getIndexBuffer
     */
    @Override
    public ConsumeFile slice(long offset) {
        return super.slice(offset * ConsumeFile.UNIT_SIZE);
    }

    /**
     * @link org.apache.rocketmq.store.ConsumeQueue#fillPreBlank
     */
    private void fillBlank(ConsumeFile mappedFile, long length) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(ConsumeFile.UNIT_SIZE);
        byteBuffer.putLong(0L);
        byteBuffer.putInt(Integer.MAX_VALUE);
        byteBuffer.putLong(0L);

        try {
            length = length % mappedFile.getFileSize();
            for (int i = 0; i < length; i += ConsumeFile.UNIT_SIZE) {
                mappedFile.append(byteBuffer);
            }
        } catch (Exception e) {
            log.error("fill blank exception", e);
        }
    }

    /**
     * @link org.apache.rocketmq.store.ConsumeQueue#rollNextFile
     */
    public long rollNextFile(long offset) {
        int count = super.fileSize / ConsumeFile.UNIT_SIZE;
        return offset + count - offset % count;
    }
}
