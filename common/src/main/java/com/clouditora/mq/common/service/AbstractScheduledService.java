package com.clouditora.mq.common.service;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Slf4j
public abstract class AbstractScheduledService extends AbstractNothingService {
    private static final long JOIN_TIME = 90 * 1000;

    protected final ScheduledExecutorService executor;
    protected final TimeUnit timeUnit;

    protected AbstractScheduledService(TimeUnit timeUnit) {
        this.timeUnit = timeUnit;
        this.executor = new ScheduledThreadPoolExecutor(1, r -> new Thread(r, getServiceName()));
    }

    protected AbstractScheduledService() {
        this(TimeUnit.MILLISECONDS);
    }

    public void scheduled(TimeUnit timeUnit, long delay, long period, Runnable runnable) {
        this.executor.scheduleWithFixedDelay(
                runnable,
                delay,
                period,
                timeUnit
        );
    }

    public void scheduled(long delay, long period, Runnable runnable) {
        scheduled(this.timeUnit, delay, period, runnable);
    }

    public void shutdown(boolean interrupt, long timeout) {
        if (!this.running.compareAndSet(true, false)) {
            log.warn("{} service not started", getServiceName());
            return;
        }
        this.executor.shutdown();
        if (!interrupt) {
            log.info("{} service shutdown", getServiceName());
            return;
        }
        try {
            boolean termination = this.executor.awaitTermination(timeout, TimeUnit.MILLISECONDS);
            if (termination) {
                log.info("{} service shutdown", getServiceName());
            } else {
                log.warn("{} service shutdown timeout", getServiceName());
                this.executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            log.error("{} service shutdown interrupted", getServiceName());
            this.executor.shutdownNow();
        }
    }

    public void shutdown(boolean interrupt) {
        shutdown(interrupt, JOIN_TIME);
    }

    @Override
    public void shutdown() {
        shutdown(false);
    }
}
