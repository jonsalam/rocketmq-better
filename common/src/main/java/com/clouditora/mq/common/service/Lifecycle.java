package com.clouditora.mq.common.service;

public interface Lifecycle {

    void startup();

    void shutdown();

    boolean isRunning();

}
