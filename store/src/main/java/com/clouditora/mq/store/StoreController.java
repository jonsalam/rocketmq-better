package com.clouditora.mq.store;

import com.clouditora.mq.common.message.MessageEntity;
import com.clouditora.mq.common.service.AbstractScheduledService;
import com.clouditora.mq.store.consume.ConsumeFile;
import com.clouditora.mq.store.consume.ConsumeQueue;
import com.clouditora.mq.store.consume.ConsumeQueueDispatcher;
import com.clouditora.mq.store.consume.ConsumeQueueManager;
import com.clouditora.mq.store.enums.GetMessageStatus;
import com.clouditora.mq.store.file.FlushType;
import com.clouditora.mq.store.file.MappedFile;
import com.clouditora.mq.store.file.PutResult;
import com.clouditora.mq.store.index.IndexFileDispatcher;
import com.clouditora.mq.store.index.IndexFileQueue;
import com.clouditora.mq.store.log.CommitLog;
import com.clouditora.mq.store.log.GetMessageResult;
import com.clouditora.mq.store.log.dispatcher.CommitLogDispatcher;
import com.clouditora.mq.store.log.flusher.CommitLogBatchFlusher;
import com.clouditora.mq.store.log.flusher.CommitLogFlusher;
import com.clouditora.mq.store.log.flusher.CommitLogScheduledFlusher;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @link org.apache.rocketmq.store.DefaultMessageStore
 */
@Slf4j
public class StoreController extends AbstractScheduledService {
    private final StoreConfig storeConfig;
    private final CommitLog commitLog;
    private final CommitLogDispatcher commitLogDispatcher;
    private final CommitLogFlusher commitLogFlusher;
    private final ConsumeQueueManager consumeQueueManager;
    private final IndexFileQueue indexFileQueue;
    protected final ScheduledExecutorService scheduledExecutor;

    public StoreController(StoreConfig storeConfig) {
        this.storeConfig = storeConfig;
        this.commitLog = new CommitLog(storeConfig, this);
        this.consumeQueueManager = new ConsumeQueueManager(storeConfig);
        IndexFileDispatcher indexFileDispatcher = new IndexFileDispatcher(storeConfig);
        ConsumeQueueDispatcher consumeQueueDispatcher = new ConsumeQueueDispatcher(this.consumeQueueManager);
        this.commitLogDispatcher = new CommitLogDispatcher(this.commitLog, consumeQueueDispatcher, indexFileDispatcher);
        if (storeConfig.getFlushDiskType() == FlushType.SYNC_FLUSH) {
            this.commitLogFlusher = new CommitLogBatchFlusher(this.commitLog);
        } else {
            this.commitLogFlusher = new CommitLogScheduledFlusher(storeConfig, this.commitLog);
        }
        this.indexFileQueue = new IndexFileQueue(storeConfig);
        this.scheduledExecutor = new ScheduledThreadPoolExecutor(1, r -> new Thread(r, getServiceName()));
    }

    @Override
    public String getServiceName() {
        return StoreController.class.getSimpleName();
    }

    /**
     * @link org.apache.rocketmq.store.DefaultMessageStore#load
     */
    @Override
    public void startup() {
        boolean normally = !abortFileExists();
        this.commitLog.map();
        this.consumeQueueManager.map();
//        this.indexFileQueue.map();
        this.consumeQueueManager.recover();
        this.commitLog.recover(normally, this.consumeQueueManager.getConsumeQueueMap());
        if (normally) {
            log.info("prev shutdown normally");
        } else {
            log.info("prev shutdown abnormally");
//            this.indexFileQueue.recover();
        }
        long minOffset = this.commitLog.getMinOffset();
        this.commitLogDispatcher.startup(minOffset);
//        this.commitLogFlusher.startup();
        super.startup();
    }

    @Override
    public void shutdown() {
        this.commitLog.unmap();
//        this.consumeQueueManager.unmap();
//        this.commitLogDispatcher.shutdown();
//        this.commitLogFlusher.shutdown();
        super.shutdown();
    }

    public void later(TimeUnit timeUnit, long delay, Runnable runnable) {
        this.scheduledExecutor.schedule(runnable, delay, timeUnit);
    }

    /**
     * @link org.apache.rocketmq.store.DefaultMessageStore#isTempFileExist
     */
    private boolean abortFileExists() {
        File file = new File(this.storeConfig.getAbortPath());
        return file.exists();
    }

    public void dispatch(MessageEntity message) {
        this.commitLogDispatcher.dispatch(message);
    }

    /**
     * org.apache.rocketmq.store.DefaultMessageStore#asyncPutMessage
     */
    public CompletableFuture<PutResult> asyncPut(MessageEntity message) {
        CompletableFuture<PutResult> result = this.commitLog.put(message);
        // 刷盘
        this.commitLogFlusher.flush(result, message);
        return result;
    }

    /**
     * @link org.apache.rocketmq.store.DefaultMessageStore#getMessage
     */
    public GetMessageResult get(String group, String topic, int queueId, long requestOffset, int requestNum) {
        GetMessageResult result = new GetMessageResult();
        ConsumeQueue queue = this.consumeQueueManager.get(topic, queueId);
        if (queue == null) {
            result.setStatus(GetMessageStatus.NO_MATCHED_LOGIC_QUEUE);
            result.setNextBeginOffset(0);
            result.setMinOffset(0);
            result.setMaxOffset(0);
            return result;
        }
        if (queue.getMaxCommitLogOffset() == 0) {
            result.setStatus(GetMessageStatus.NO_MESSAGE_IN_QUEUE);
            result.setNextBeginOffset(0);
        } else if (requestOffset == queue.getMaxCommitLogOffset()) {
            result.setStatus(GetMessageStatus.OFFSET_OVER);
            result.setNextBeginOffset(requestOffset);
        } else if (requestOffset > queue.getMaxCommitLogOffset()) {
            result.setStatus(GetMessageStatus.OFFSET_OVERFLOW);
            result.setNextBeginOffset(queue.getMaxCommitLogOffset());
        } else {
            ConsumeFile consumeFile = queue.slice(requestOffset);
            if (consumeFile == null) {
                result.setStatus(GetMessageStatus.OFFSET_FOUND_NULL);
                result.setNextBeginOffset(queue.rollNextFile(requestOffset));
            } else {
                int maxOffset = Math.max(16000, requestNum * ConsumeFile.UNIT_SIZE);
                List<MappedFile> slices = new ArrayList<>(requestNum);
                for (int p = 0; p < consumeFile.getWritePosition() && p < maxOffset; p += ConsumeFile.UNIT_SIZE) {
                    ByteBuffer byteBuffer = consumeFile.getByteBuffer();
                    long offset = byteBuffer.getLong();
                    int messageSize = byteBuffer.getInt();
                    long tagCode = byteBuffer.getLong();


                    MappedFile commitLog = this.commitLog.slice(offset, messageSize);
                    if (commitLog == null) {
                        continue;
                    }
                    slices.add(commitLog);
                }
            }
        }
        return result;
    }
}
