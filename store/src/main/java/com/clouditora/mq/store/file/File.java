package com.clouditora.mq.store.file;

public interface File {
    void mapped();

    void unmapped();

    void delete();

    void flush(int pages);
}
