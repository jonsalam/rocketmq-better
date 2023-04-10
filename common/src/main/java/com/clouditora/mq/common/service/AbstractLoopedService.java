package com.clouditora.mq.common.service;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractLoopedService extends AbstractSimpleService {

    @Override
    protected void run() {
        while (isRunning()) {
            try {
                loop();
            } catch (InterruptedException e) {
                log.warn("{} service interrupted", getServiceName());
            } catch (Exception e) {
                log.error("{} service exception", getServiceName(), e);
            }
        }
        onShutdown();
    }

    @Override
    public void shutdown() {
        shutdown(true, JOIN_TIME);
    }

    protected abstract void loop() throws Exception;

    protected void onShutdown() {

    }
}
