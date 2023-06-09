package com.clouditora.mq.common.service;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Slf4j
public abstract class AbstractLaterService extends AbstractLoopedService {
    protected final ScheduledExecutorService scheduledExecutor;
    @Setter
    protected TimeUnit timeUnit = TimeUnit.MILLISECONDS;
    @Setter
    protected long delay = 0;

    public AbstractLaterService() {
        this.scheduledExecutor = new ScheduledThreadPoolExecutor(1, r -> new Thread(r, getServiceName()));
    }

    public AbstractLaterService(TimeUnit timeUnit, long delay) {
        this();
        this.timeUnit = timeUnit;
        this.delay = delay;
    }

    public void later(TimeUnit timeUnit, long delay, Runnable runnable) {
        this.scheduledExecutor.schedule(runnable, delay, timeUnit);
    }

    public void later(long delay, Runnable runnable) {
        if (this.timeUnit == null) {
            log.error("{} time unit is null", getServiceName());
            throw new RuntimeException("time unit is null");
        }
        later(this.timeUnit, delay, runnable);
    }

    public void later(Runnable runnable) {
        later(this.timeUnit, this.delay, runnable);
    }
}
