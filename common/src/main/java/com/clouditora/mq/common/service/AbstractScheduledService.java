package com.clouditora.mq.common.service;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Slf4j
public abstract class AbstractScheduledService extends AbstractNothingService {
    private static final long JOIN_TIME = 90 * 1000;

    protected final ScheduledExecutorService timer;

    protected final TimeUnit timeUnit;
    protected final long initialDelay;
    protected final long delay;

    protected AbstractScheduledService(TimeUnit timeUnit, long initialDelay, long delay) {
        this.timeUnit = timeUnit;
        this.initialDelay = initialDelay;
        this.delay = delay;
        this.timer = new ScheduledThreadPoolExecutor(1, r -> new Thread(r, getServiceName()));
    }

    @Override
    public void startup() {
        if (isRunning()) {
            log.warn("{} service already started", getServiceName());
            return;
        }

        this.timer.scheduleWithFixedDelay(
                () -> AbstractScheduledService.this.run(),
                this.initialDelay,
                this.delay,
                this.timeUnit
        );

        if (this.running.compareAndSet(false, true)) {
            log.info("{} service startup", getServiceName());
        }
    }

    public void shutdown(boolean interrupt, long timeout) {
        if (!this.running.compareAndSet(true, false)) {
            log.warn("{} service not started", getServiceName());
            return;
        }
        if (interrupt) {
            try {
                boolean termination = timer.awaitTermination(timeout, TimeUnit.MILLISECONDS);
                if (termination) {
                    log.info("{} service shutdown", getServiceName());
                } else {
                    log.warn("{} service shutdown timeout", getServiceName());
                }
            } catch (InterruptedException e) {
                log.error("{} service shutdown InterruptedException", getServiceName());
            }
        } else {
            this.timer.shutdown();
        }
    }

    public void shutdown(boolean interrupt) {
        shutdown(interrupt, JOIN_TIME);
    }

    @Override
    public void shutdown() {
        shutdown(false);
    }

    protected abstract void run();
}
