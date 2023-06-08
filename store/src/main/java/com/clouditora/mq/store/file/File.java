package com.clouditora.mq.store.file;

public interface File {
    void map();

    /**
     * @link org.apache.rocketmq.store.ReferenceResource#cleanup
     */
    void unmap();

    /**
     * @link org.apache.rocketmq.store.MappedFile#destroy
     */
    void delete();

    /**
     * @param pages 0表示只要有写入就刷盘
     */
    void flush(int pages);
}
