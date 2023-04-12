package com.clouditora.mq.network.netty;

import lombok.Data;

@Data
public class NetworkConfig {
    protected int getWorkerThreads() {
        return Runtime.getRuntime().availableProcessors();
    }

    protected int getCallbackExecutorThreads() {
        return Runtime.getRuntime().availableProcessors();
    }
}
