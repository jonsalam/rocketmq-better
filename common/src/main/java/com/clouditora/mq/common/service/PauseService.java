package com.clouditora.mq.common.service;

public interface PauseService {
    boolean isPause();

    void pause();

    void recover();
}
