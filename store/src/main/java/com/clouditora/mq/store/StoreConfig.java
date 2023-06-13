package com.clouditora.mq.store;

import com.clouditora.mq.common.constant.GlobalConstant;
import com.clouditora.mq.store.consume.ConsumeFile;
import com.clouditora.mq.store.file.FlushType;
import lombok.Data;

@Data
public class StoreConfig {
    private String home = System.getProperty(GlobalConstant.ROCKETMQ_HOME_PROPERTY, System.getenv(GlobalConstant.ROCKETMQ_HOME_ENV));
    private String rootPath = "%s/store".formatted(home);
    /**
     * CommitLog file size, default is 1G
     */
    private int commitLogFileSize = 1024 * 1024 * 1024;
    /**
     * ConsumeQueue file size, default is 30W
     */
    private int consumeQueueFileSize = 30_0000 * ConsumeFile.UNIT_SIZE;
    private int maxSlotCount = 500_0000;
    private int maxItemCount = 500_0000 * 4;

    private int flushIntervalCommitLog = 500;
    private int flushCommitLogLeastPages = 4;
    private int flushCommitLogThoroughInterval = 1000 * 10;
    private FlushType flushDiskType = FlushType.ASYNC_FLUSH;

    private String commitLogPath = "%s/commitlog".formatted(rootPath);
    /**
     * @link org.apache.rocketmq.store.config.StorePathConfigHelper#getStorePathConsumeQueue
     */
    private String consumeQueuePath = "%s/consumequeue".formatted(rootPath);
    /**
     * @link org.apache.rocketmq.store.config.StorePathConfigHelper#getStorePathIndex
     */
    private String indexPath = "%s/index".formatted(rootPath);
    /**
     * @link org.apache.rocketmq.store.config.StorePathConfigHelper#getAbortFile
     */
    private String abortPath = "%s/abort".formatted(rootPath);
    /**
     * @link org.apache.rocketmq.store.config.StorePathConfigHelper#getStoreCheckpoint
     */
    private String checkpointPath = "%s/checkpoint".formatted(rootPath);
    /**
     * @link org.apache.rocketmq.store.config.StorePathConfigHelper#getLockFile
     */
    private String lockPath = "%s/lock".formatted(rootPath);
}
