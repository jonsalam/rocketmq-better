package com.clouditora.mq.common.service;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Slf4j
public abstract class AbstractScheduledService extends AbstractNothingService {
    private static final long JOIN_TIME = 90 * 1000;

    protected final ScheduledExecutorService scheduledExecutor;
    protected final TimeUnit timeUnit;
    protected final long delay;
    protected final long period;

    protected AbstractScheduledService(TimeUnit timeUnit, long delay, long period) {
        this.timeUnit = timeUnit;
        this.delay = delay;
        this.period = period;
        this.scheduledExecutor = new ScheduledThreadPoolExecutor(1, r -> new Thread(r, getServiceName()));
    }

    @Override
    protected void init() throws Exception {
        this.scheduledExecutor.scheduleWithFixedDelay(
                () -> {
                    try {
                        AbstractScheduledService.this.run();
                    } catch (Exception e) {
                        log.warn("{} service exception", getServiceName(), e);
                    }
                },
                this.delay,
                this.period,
                this.timeUnit
        );
    }

    public void shutdown(boolean interrupt, long timeout) {
        if (!this.running.compareAndSet(true, false)) {
            log.warn("{} service not started", getServiceName());
            return;
        }
        this.scheduledExecutor.shutdown();
        if (interrupt) {
            try {
                boolean termination = this.scheduledExecutor.awaitTermination(timeout, TimeUnit.MILLISECONDS);
                if (termination) {
                    log.info("{} service shutdown", getServiceName());
                } else {
                    log.warn("{} service shutdown timeout", getServiceName());
                    this.scheduledExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                log.error("{} service shutdown interrupted", getServiceName());
                this.scheduledExecutor.shutdownNow();
            }
        }
    }

    public void shutdown(boolean interrupt) {
        shutdown(interrupt, JOIN_TIME);
    }

    @Override
    public void shutdown() {
        shutdown(false);
    }

    protected abstract void run() throws Exception;
}
