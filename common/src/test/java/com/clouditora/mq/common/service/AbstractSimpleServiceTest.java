package com.clouditora.mq.common.service;

import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AbstractSimpleServiceTest {

    @Test
    void startup() {
        AbstractSimpleService service = new AbstractSimpleService() {
            @Override
            public String getServiceName() {
                return "test";
            }

            @Override
            protected void run() {

            }
        };

        assertFalse(service.isRunning());
        service.startup();
        assertTrue(service.isRunning());
    }

    @Test
    void shutdown() throws InterruptedException {
        AbstractSimpleService service = new AbstractSimpleService() {
            @Override
            public String getServiceName() {
                return "test";
            }

            @Override
            protected void run() {
                try {
                    TimeUnit.MILLISECONDS.sleep(10);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        };
        service.startup();
        service.shutdown();
        assertFalse(service.isRunning());
    }
}
