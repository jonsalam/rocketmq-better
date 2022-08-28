package com.clouditora.mq.common.service;

import com.clouditora.mq.common.concurrent.ResettableCountDownLatch;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public abstract class AbstractWaitService extends AbstractSimpleService {
    protected final ResettableCountDownLatch countDownLatch = new ResettableCountDownLatch(1);
    protected volatile AtomicBoolean waken = new AtomicBoolean(false);
    @Setter
    protected long interval = 60_000;

    @Override
    public void shutdown(boolean interrupt, long timeout) {
        if (!isRunning()) {
            log.warn("{} service not started", getServiceName());
            return;
        }
        if (this.waken.compareAndSet(false, true)) {
            log.warn("{} service shutdown and wakeup last once", getServiceName());
            this.countDownLatch.countDown();
        }
        super.shutdown(interrupt, timeout);
    }

    public void wakeup() {
        if (this.waken.compareAndSet(false, true)) {
            log.info("{} service wait up by thread: {}", getServiceName(), Thread.currentThread().getName());
            this.countDownLatch.countDown();
        }
    }

    protected void await() {
        log.info("{} service wait for wake up, waken={}, timeout={}mS", getServiceName(), waken, this.interval);
        if (this.waken.compareAndSet(true, false)) {
            log.info("{} service woken up for waken", getServiceName());
            onWakeup();
            return;
        }

        try {
            boolean result = this.countDownLatch.await(this.interval, TimeUnit.MILLISECONDS);
            if (result) {
                log.info("{} service woken up for wait up", getServiceName());
            } else {
                log.info("{} service woken up for wait timeout", getServiceName());
            }
        } catch (InterruptedException e) {
            log.error("{} service woken up for interrupted", getServiceName());
        } finally {
            onWakeup();
            this.countDownLatch.reset();
            this.waken.set(false);
        }
    }

    @Override
    protected void run() {
        while (isRunning()) {
            try {
                onPreWakeup();
                await();
                onPostWakeup();
            } catch (Exception e) {
                log.error("{} service exception", getServiceName(), e);
            }
        }
        onShutdown();
    }

    protected void onPreWakeup() {

    }

    protected abstract void onWakeup();

    protected void onPostWakeup() {

    }

    protected void onShutdown() {
        if (waken.get()) {
            log.warn("{} service shutdown but need wake up last once", getServiceName());
            onWakeup();
        }
    }
}
