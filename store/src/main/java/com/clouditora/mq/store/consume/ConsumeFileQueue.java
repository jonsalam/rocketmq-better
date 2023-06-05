package com.clouditora.mq.store.consume;

import com.clouditora.mq.store.MessageStoreConfig;
import com.clouditora.mq.store.file.MappedFileQueue;
import com.clouditora.mq.store.util.StoreUtil;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @link org.apache.rocketmq.store.ConsumeQueue
 */
@Slf4j
public class ConsumeFileQueue extends MappedFileQueue<ConsumeFile> {
    private final MessageStoreConfig config;
    private final String topic;
    private final int queueId;
    @Getter
    private volatile long minOffset = 0;
    @Getter
    private volatile long maxOffset = 0;

    public ConsumeFileQueue(MessageStoreConfig config, String topic, int queueId) {
        super("%s/%s/%s".formatted(config.getConsumeQueuePath(), topic, queueId), config.getConsumeQueueFileSize());
        this.config = config;
        this.topic = topic;
        this.queueId = queueId;
    }

    public void increaseMaxOffset(int size) {
        this.maxOffset += size;
    }

    @Override
    protected ConsumeFile create(long offset) throws IOException {
        String path = "%s/%s".formatted(super.path, StoreUtil.long2String(offset));
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
        if (offset < file.getWritePosition() + file.getStartOffset()) {
            log.warn("create consume file repeatedly: expectOffset={}, currentOffset={}", offset, file.getWritePosition() + file.getStartOffset());
            return null;
        }
        return file;
    }

    /**
     * @link org.apache.rocketmq.store.ConsumeQueue#fillPreBlank
     */
    private void fillBlank(ConsumeFile mappedFile, long untilWhere) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(ConsumeFile.UNIT_SIZE);
        byteBuffer.putLong(0L);
        byteBuffer.putInt(Integer.MAX_VALUE);
        byteBuffer.putLong(0L);

        try {
            long num = untilWhere % mappedFile.getFileSize();
            for (int i = 0; i < num; i += ConsumeFile.UNIT_SIZE) {
                mappedFile.append(byteBuffer);
            }
        } catch (Exception e) {
            log.error("fill blank exception", e);
        }
    }

    /**
     * @link org.apache.rocketmq.store.ConsumeQueue#getIndexBuffer
     */
    @Override
    public ConsumeFile slice(long offset) {
        return super.slice(offset * ConsumeFile.UNIT_SIZE);
    }

    /**
     * @link org.apache.rocketmq.store.ConsumeQueue#rollNextFile
     */
    public long rollNextFile(long offset) {
        int count = super.fileSize / ConsumeFile.UNIT_SIZE;
        return offset + count - offset % count;
    }
}
