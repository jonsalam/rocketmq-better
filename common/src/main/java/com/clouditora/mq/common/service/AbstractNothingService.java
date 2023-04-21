package com.clouditora.mq.common.service;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @link org.apache.rocketmq.common.ServiceThread
 */
@Slf4j
public abstract class AbstractNothingService implements Lifecycle {
    protected final AtomicBoolean running = new AtomicBoolean(false);

    public abstract String getServiceName();

    protected void init() throws Exception {

    }

    @Override
    public void startup() {
        if (isRunning()) {
            log.warn("{} service already started", getServiceName());
            return;
        }
        try {
            init();
        } catch (Exception e) {
            log.error("{} service startup exception", getServiceName(), e);
            System.exit(-1);
        }
        if (this.running.compareAndSet(false, true)) {
            log.info("{} service startup", getServiceName());
        } else {
            log.error("{} service startup failed", getServiceName());
            System.exit(-1);
        }
    }

    @Override
    public boolean isRunning() {
        return this.running.get();
    }

    @Override
    public void shutdown() {
        if (!this.running.compareAndSet(true, false)) {
            log.warn("{} service not started", getServiceName());
        }
    }
}
