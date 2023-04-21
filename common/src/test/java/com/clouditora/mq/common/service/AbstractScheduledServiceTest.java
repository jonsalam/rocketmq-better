package com.clouditora.mq.common.service;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
class AbstractScheduledServiceTest {

    @Test
    void startup() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(5);
        AbstractScheduledService service = new AbstractScheduledService() {
            @Override
            public String getServiceName() {
                return "schedule";
            }

            @Override
            public void startup() {
                register(TimeUnit.MICROSECONDS, 10, 10, latch::countDown);
            }
        };
        service.startup();
        latch.await();
        assertTrue(service.isRunning());
        service.shutdown();
        assertFalse(service.isRunning());
    }
}
