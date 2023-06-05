package com.clouditora.mq.store;

import com.clouditora.mq.common.constant.GlobalConstant;
import com.clouditora.mq.store.consume.ConsumeFile;
import lombok.Data;

@Data
public class MessageStoreConfig {
    private String home = System.getProperty(GlobalConstant.ROCKETMQ_HOME_PROPERTY, System.getenv(GlobalConstant.ROCKETMQ_HOME_ENV));
    private String rootPath = "%s/store".formatted(home);
    // CommitLog file size, default is 1G
    private int commitLogFileSize = 1024 * 1024 * 1024;
    // ConsumeQueue file size, default is 30W
    private int consumeQueueFileSize = 30_0000 * ConsumeFile.UNIT_SIZE;
    private int maxSlotCount = 500_0000;
    private int maxItemCount = 500_0000 * 4;

    public String getCommitLogPath() {
        return "%s/commitlog".formatted(rootPath);
    }

    /**
     * @link org.apache.rocketmq.store.config.StorePathConfigHelper#getStorePathConsumeQueue
     */
    public String getConsumeQueuePath() {
        return "%s/consumequeue".formatted(rootPath);
    }

    /**
     * @link org.apache.rocketmq.store.config.StorePathConfigHelper#getStorePathIndex
     */
    public String getIndexPath() {
        return "%s/index".formatted(rootPath);
    }
}
