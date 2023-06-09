package com.clouditora.mq.store.log.flusher;

import com.clouditora.mq.common.service.AbstractWaitService;
import com.clouditora.mq.store.enums.PutMessageStatus;
import com.clouditora.mq.store.log.CommitLog;
import lombok.Getter;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @link org.apache.rocketmq.store.CommitLog.GroupCommitService
 */
public class CommitLogGroupFlusher extends AbstractWaitService implements CommitLogFlusher {
    private final CommitLog commitLog;
    private volatile List<FlushFuture> writeRequests = new LinkedList<>();
    private volatile List<FlushFuture> readRequests = new LinkedList<>();
    private final ReentrantLock lock = new ReentrantLock();

    public CommitLogGroupFlusher(CommitLog commitLog) {
        super.interval = 10;
        this.commitLog = commitLog;
    }

    @Override
    public String getServiceName() {
        return CommitLogGroupFlusher.class.getSimpleName();
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
                request.wakeupCustomer(flushed ? PutMessageStatus.SUCCESS : PutMessageStatus.FLUSH_DISK_TIMEOUT);
            }
            this.readRequests = new LinkedList<>();
        }
    }

    public void putRequest(long offset) {
        try {
            this.lock.lock();
            FlushFuture request = new FlushFuture(offset);
            this.writeRequests.add(request);
        } finally {
            this.lock.unlock();
        }
        this.wakeup();
    }

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

    /**
     * @link org.apache.rocketmq.store.CommitLog.GroupCommitRequest
     */
    static class FlushFuture {
        @Getter
        private final long offset;
        private final CompletableFuture<PutMessageStatus> future = new CompletableFuture<>();

        public FlushFuture(long offset) {
            this.offset = offset;
        }

        public void wakeupCustomer(PutMessageStatus putMessageStatus) {
            this.future.complete(putMessageStatus);
        }
    }
}
