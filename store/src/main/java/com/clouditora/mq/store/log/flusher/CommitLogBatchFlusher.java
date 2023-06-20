package com.clouditora.mq.store.log.flusher;

import com.clouditora.mq.common.MessageConst;
import com.clouditora.mq.common.message.MessageEntity;
import com.clouditora.mq.common.service.AbstractWaitService;
import com.clouditora.mq.store.file.PutResult;
import com.clouditora.mq.store.file.PutStatus;
import com.clouditora.mq.store.log.CommitLog;
import lombok.Getter;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @link org.apache.rocketmq.store.CommitLog.GroupCommitService
 */
public class CommitLogBatchFlusher extends AbstractWaitService implements CommitLogFlusher {
    private final CommitLog commitLog;
    private volatile List<FlushFuture> writeRequests = new LinkedList<>();
    private volatile List<FlushFuture> readRequests = new LinkedList<>();
    private final ReentrantLock lock = new ReentrantLock();

    public CommitLogBatchFlusher(CommitLog commitLog) {
        super.interval = 10;
        this.commitLog = commitLog;
    }

    @Override
    public String getServiceName() {
        return CommitLogBatchFlusher.class.getSimpleName();
    }

    @Override
    protected void onWakeup() {
        swapWriteAndRead();
    }

    @Override
    protected void onPostWakeup() {
        flush();
    }

    @Override
    public void shutdown(boolean interrupt) {
        super.shutdown(interrupt, 5 * 60 * 1000);
    }

    @Override
    protected void onShutdown() {
        try {
            Thread.sleep(10);
        } catch (Exception ignored) {
        }
        swapWriteAndRead();
        flush();
    }

    /**
     * @link org.apache.rocketmq.store.CommitLog.GroupCommitService#swapRequests
     */
    private void swapWriteAndRead() {
        try {
            this.lock.lock();
            List<FlushFuture> tmp = this.writeRequests;
            this.writeRequests = this.readRequests;
            this.readRequests = tmp;
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public CompletableFuture<PutStatus> flush(CompletableFuture<PutResult> result, MessageEntity message) {
        Boolean stored = message.getProperty(MessageConst.Property.WAIT_STORE_OK, Boolean.class, true);
        if (stored) {
            return putRequest(message.getCommitLogOffset() + message.getMessageLength());
        } else {
            wakeup();
            return CompletableFuture.completedFuture(PutStatus.SUCCESS);
        }
    }

    /**
     * @link org.apache.rocketmq.store.CommitLog.GroupCommitService#putRequest
     */
    public CompletableFuture<PutStatus> putRequest(long offset) {
        try {
            this.lock.lock();
            FlushFuture request = new FlushFuture(offset);
            this.writeRequests.add(request);
            this.wakeup();
            return request.future;
        } finally {
            this.lock.unlock();
        }
    }

    /**
     * @link org.apache.rocketmq.store.CommitLog.GroupCommitRequest
     */
    static class FlushFuture {
        @Getter
        private final long offset;
        private final CompletableFuture<PutStatus> future = new CompletableFuture<>();

        public FlushFuture(long offset) {
            this.offset = offset;
        }

        public void wakeupCustomer(PutStatus status) {
            this.future.complete(status);
        }
    }

    /**
     * @link org.apache.rocketmq.store.CommitLog.GroupCommitService#doCommit
     */
    private void flush() {
        if (this.readRequests.isEmpty()) {
            // Because of individual messages is set to not sync flush, it will come to this process
            this.commitLog.flush(0);
        } else {
            for (FlushFuture request : this.readRequests) {
                boolean flushed = this.commitLog.getFlushOffset() >= request.getOffset();
                for (int i = 0; i < 2 && !flushed; i++) {
                    long flushPosition = this.commitLog.flush(0);
                    flushed = flushPosition >= request.getOffset();
                }
                request.wakeupCustomer(flushed ? PutStatus.SUCCESS : PutStatus.FLUSH_DISK_TIMEOUT);
            }
            this.readRequests = new LinkedList<>();
        }
    }
}
