package com.clouditora.mq.store.index;

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

    public void incrMaxOffset(int size) {
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
        if (super.files.get(0) == file && file.getWritePosition() == 0 && offset != 0) {
//            this.mappedFileQueue.setFlushedWhere(expectLogicOffset);
//            this.mappedFileQueue.setCommittedWhere(expectLogicOffset);
            fillBlank(file, offset);
            this.minOffset = offset;
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

    @Override
    public ConsumeFile slice(long logOffset) {
        long offset = logOffset * ConsumeFile.UNIT_SIZE;
        return super.slice(offset);
    }
}
