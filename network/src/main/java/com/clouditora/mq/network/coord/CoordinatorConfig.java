package com.clouditora.mq.network.coord;

import lombok.Data;

@Data
public class CoordinatorConfig {
    protected int getWorkerThreads() {
        return Runtime.getRuntime().availableProcessors();
    }

    protected int getCallbackExecutorThreads() {
        return Runtime.getRuntime().availableProcessors();
    }
}
