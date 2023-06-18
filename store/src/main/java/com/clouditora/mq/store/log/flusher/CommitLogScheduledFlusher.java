package com.clouditora.mq.store.log.flusher;

import com.clouditora.mq.common.message.MessageEntity;
import com.clouditora.mq.common.service.AbstractScheduledService;
import com.clouditora.mq.store.StoreConfig;
import com.clouditora.mq.store.file.PutResult;
import com.clouditora.mq.store.file.PutStatus;
import com.clouditora.mq.store.log.CommitLog;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;

/**
 * @link org.apache.rocketmq.store.CommitLog.FlushRealTimeService
 */
@Slf4j
public class CommitLogScheduledFlusher extends AbstractScheduledService implements CommitLogFlusher {
    protected static final int RETRY_TIMES_OVER = 10;

    private final StoreConfig storeConfig;
    private final CommitLog commitLog;

    private long flushTimestamp = 0;

    public CommitLogScheduledFlusher(StoreConfig storeConfig, CommitLog commitLog) {
        this.storeConfig = storeConfig;
        this.commitLog = commitLog;
    }

    @Override
    public String getServiceName() {
        return CommitLogScheduledFlusher.class.getSimpleName();
    }

    @Override
    public void startup() {
        int period = this.storeConfig.getFlushIntervalCommitLog();
        scheduled(period, period, () -> {
            int interval = this.storeConfig.getFlushCommitLogThoroughInterval();
            int pages = this.storeConfig.getFlushCommitLogLeastPages();
            long now = System.currentTimeMillis();
            if (now >= flushTimestamp + interval) {
                // 每隔一段时间, 刷新全量数据
                this.flushTimestamp = now;
                pages = 0;
            }
            this.commitLog.flush(pages);
        });
        super.startup();
    }

    @Override
    public void shutdown(boolean interrupt, long timeout) {
        for (int i = 0; i < RETRY_TIMES_OVER; i++) {
            this.commitLog.flush(0);
        }
        super.shutdown(interrupt, timeout);
    }

    @Override
    public CompletableFuture<PutStatus> flush(CompletableFuture<PutResult> result, MessageEntity message) {
        return CompletableFuture.completedFuture(PutStatus.SUCCESS);
    }
}
