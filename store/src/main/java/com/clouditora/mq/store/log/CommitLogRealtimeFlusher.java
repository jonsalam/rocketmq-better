package com.clouditora.mq.store.log;

import com.clouditora.mq.common.service.AbstractLoopedService;
import com.clouditora.mq.store.MessageStoreConfig;
import com.clouditora.mq.store.file.MappedFile;
import com.clouditora.mq.store.file.MappedFileQueue;
import lombok.extern.slf4j.Slf4j;

/**
 * @link org.apache.rocketmq.store.CommitLog.FlushRealTimeService
 */
@Slf4j
public class CommitLogRealtimeFlusher extends AbstractLoopedService implements CommitLogFlusher {
    protected static final int RETRY_TIMES_OVER = 10;

    private final MessageStoreConfig messageStoreConfig;
    private final MappedFileQueue<MappedFile> commitLogQueue;

    public CommitLogRealtimeFlusher(MessageStoreConfig messageStoreConfig, MappedFileQueue<MappedFile> commitLogQueue) {
        this.messageStoreConfig = messageStoreConfig;
        this.commitLogQueue = commitLogQueue;
    }

    @Override
    public String getServiceName() {
        return CommitLogRealtimeFlusher.class.getSimpleName();
    }

    @Override
    protected void loop() throws Exception {
        int flushCommitLogLeastPages = this.messageStoreConfig.getFlushCommitLogLeastPages();
        this.commitLogQueue.flush(flushCommitLogLeastPages);
    }

    @Override
    protected void onShutdown() {
        for (int i = 0; i < RETRY_TIMES_OVER; i++) {
            this.commitLogQueue.flush(0);
        }
    }
}
