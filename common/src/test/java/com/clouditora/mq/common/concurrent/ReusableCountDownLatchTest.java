package com.clouditora.mq.common.concurrent;

import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReusableCountDownLatchTest {

    @Test
    void await_timeout() throws InterruptedException {
        ResettableCountDownLatch latch = new ResettableCountDownLatch(1);
        boolean await = latch.await(10, TimeUnit.MILLISECONDS);
        assertFalse(await);

        latch.countDown();
        await = latch.await(10, TimeUnit.MILLISECONDS);
        assertTrue(await);
    }

    @Test
    void reset() throws InterruptedException {
        ResettableCountDownLatch latch = new ResettableCountDownLatch(1);

        assertEquals(1, latch.getCount());
        latch.countDown();
        assertEquals(0, latch.getCount());

        latch.reset();

        assertEquals(1, latch.getCount());
        latch.countDown();
        assertEquals(0, latch.getCount());
    }
}
