package com.clouditora.mq.common.concurrent;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;

/**
 * @link org.apache.rocketmq.common.CountDownLatch2
 */
public class ResettableCountDownLatch {
    private final Synchronizer synchronizer;

    public ResettableCountDownLatch(int count) {
        if (count < 0) {
            throw new IllegalArgumentException("count < 0");
        }
        this.synchronizer = new Synchronizer(count);
    }

    public void reset() {
        this.synchronizer.reset();
    }

    public void await() throws InterruptedException {
        this.synchronizer.acquireSharedInterruptibly(1);
    }

    public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
        return this.synchronizer.tryAcquireSharedNanos(1, unit.toNanos(timeout));
    }

    public void countDown() {
        this.synchronizer.releaseShared(1);
    }

    public long getCount() {
        return this.synchronizer.getCount();
    }

    @Override
    public String toString() {
        return super.toString() + "[Count = " + this.synchronizer.getCount() + "]";
    }

    private static final class Synchronizer extends AbstractQueuedSynchronizer {
        private final int count;

        Synchronizer(int count) {
            this.count = count;
            super.setState(count);
        }

        private int getCount() {
            return super.getState();
        }

        private void reset() {
            super.setState(count);
        }

        @Override
        protected int tryAcquireShared(int acquires) {
            return (getCount() == 0) ? 1 : -1;
        }

        @Override
        protected boolean tryReleaseShared(int releases) {
            for (; ; ) {
                int count = getCount();
                if (count == 0) {
                    return false;
                }
                int next = count - 1;
                if (super.compareAndSetState(count, next)) {
                    return next == 0;
                }
            }
        }
    }
}
