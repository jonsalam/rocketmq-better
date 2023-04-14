package com.clouditora.mq.common.service;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractSimpleService extends AbstractNothingService {
    protected static final long JOIN_TIME = 90 * 1000;
    @Setter
    protected boolean daemon = false;
    /**
     * Make it able to restart the thread
     */
    protected Thread thread;

    @Override
    protected void init() throws Exception {
        this.thread = new Thread(() -> AbstractSimpleService.this.run(), getServiceName());
        this.thread.setDaemon(this.daemon);
    }

    @Override
    public void startup() {
        super.startup();
        this.thread.start();
    }

    public void shutdown(boolean interrupt, long timeout) {
        if (!this.running.compareAndSet(true, false)) {
            log.warn("{} service not started", getServiceName());
            return;
        }
        try {
            log.info("{} service shutdown, interrupt={}, timeout={}mS", getServiceName(), interrupt, timeout);
            if (interrupt) {
                this.thread.interrupt();
            }
            if (!this.thread.isDaemon()) {
                this.thread.join(timeout);
            }
            log.info("{} service stopped", getServiceName());
        } catch (InterruptedException e) {
            log.error("{} service shutdown interrupted", getServiceName(), e);
        }
    }

    public void shutdown(boolean interrupt) {
        shutdown(interrupt, JOIN_TIME);
    }

    @Override
    public void shutdown() {
        shutdown(false, JOIN_TIME);
    }

    protected abstract void run();
}
