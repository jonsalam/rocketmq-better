package com.clouditora.mq.network.coord;

import org.junit.jupiter.api.Test;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertNull;

class CommandCleanerTest {

    @Test
    void test() throws InterruptedException {
        ConcurrentMap<Integer, CommandFuture> commandMap = new ConcurrentHashMap<>();
        CommandFuture commandFuture = new CommandFuture(null, 0, 100, null);
        commandMap.put(0, commandFuture);

        CommandCleaner cleaner = new CommandCleaner(commandMap, null) {
            @Override
            protected void init() {
                register(10, 10, this::cleanTimeoutCommand);
            }

            @Override
            public ExecutorService getCallbackExecutor() {
                return null;
            }
        };
        cleaner.startup();

        TimeUnit.MILLISECONDS.sleep(1100);
        cleaner.shutdown();
        assertNull(commandMap.get(0));
    }

}
