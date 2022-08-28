package com.clouditora.mq.common.service;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
class AbstractWaitServiceTest {

    @Test
    void shutdown() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AbstractWaitService service = new AbstractWaitService() {
            @Override
            public String getServiceName() {
                return "test";
            }

            @Override
            protected void onWakeup() {
                log.info("onWakeup: latch={}", latch);
                latch.countDown();
                log.info("onWakeup: latch={}", latch);
            }
        };
        service.startup();
        assertTrue(service.isRunning());
        // 停止的时候, 会主动唤醒一次
        service.shutdown();

        latch.await();
        assertFalse(service.isRunning());
    }

    @Test
    void wakeup() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AbstractWaitService service = new AbstractWaitService() {
            @Override
            public String getServiceName() {
                return "test";
            }

            @Override
            protected void onWakeup() {
                log.info("onWakeup: waken={}, latch={}", waken, latch);
                latch.countDown();
                log.info("onWakeup: waken={}, latch={}", waken, latch);
            }
        };
        service.startup();
        assertTrue(service.isRunning());

        service.wakeup();

        latch.await();
        service.shutdown();
        assertFalse(service.isRunning());
    }
}
