package com.clouditora.mq.store;

import com.clouditora.mq.common.message.MessageEntity;
import com.clouditora.mq.common.service.AbstractNothingService;
import com.clouditora.mq.store.consume.ConsumeDispatcher;
import com.clouditora.mq.store.consume.ConsumeFile;
import com.clouditora.mq.store.consume.ConsumeQueue;
import com.clouditora.mq.store.consume.ConsumeQueueManager;
import com.clouditora.mq.store.enums.GetMessageStatus;
import com.clouditora.mq.store.file.FlushType;
import com.clouditora.mq.store.file.GetMessageResult;
import com.clouditora.mq.store.file.MappedFile;
import com.clouditora.mq.store.file.PutResult;
import com.clouditora.mq.store.index.IndexFileDispatcher;
import com.clouditora.mq.store.log.CommitLog;
import com.clouditora.mq.store.log.dispatcher.CommitLogDispatcher;
import com.clouditora.mq.store.log.flusher.CommitLogFlusher;
import com.clouditora.mq.store.log.flusher.CommitLogGroupFlusher;
import com.clouditora.mq.store.log.flusher.CommitLogScheduledFlusher;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * @link org.apache.rocketmq.store.DefaultMessageStore
 */
@Slf4j
public class StoreController extends AbstractNothingService {
    private final StoreConfig storeConfig;
    private final CommitLog commitLog;
    private final CommitLogDispatcher commitLogDispatcher;
    private final CommitLogFlusher commitLogFlusher;
    private final ConsumeQueueManager consumeQueueManager;

    public StoreController(StoreConfig storeConfig) {
        this.storeConfig = storeConfig;
        this.commitLog = new CommitLog(storeConfig, this);
        this.consumeQueueManager = new ConsumeQueueManager(storeConfig);
        IndexFileDispatcher indexFileDispatcher = new IndexFileDispatcher(storeConfig);
        ConsumeDispatcher consumeDispatcher = new ConsumeDispatcher(this.consumeQueueManager);
        this.commitLogDispatcher = new CommitLogDispatcher(this.commitLog, consumeDispatcher, indexFileDispatcher);
        if (storeConfig.getFlushDiskType() == FlushType.SYNC_FLUSH) {
            this.commitLogFlusher = new CommitLogGroupFlusher(this.commitLog);
        } else {
            this.commitLogFlusher = new CommitLogScheduledFlusher(storeConfig, this.commitLog);
        }
    }

    @Override
    public String getServiceName() {
        return StoreController.class.getSimpleName();
    }

    @Override
    public void startup() {
        this.commitLogDispatcher.startup();
        this.commitLogFlusher.startup();
        super.startup();
    }

    @Override
    public void shutdown() {
        this.commitLogDispatcher.shutdown();
        this.commitLogFlusher.shutdown();
        super.shutdown();
    }

    public void dispatch(MessageEntity message) {
        this.commitLogDispatcher.dispatch(message);
    }

    /**
     * org.apache.rocketmq.store.DefaultMessageStore#asyncPutMessage
     */
    public CompletableFuture<PutResult> asyncPut(MessageEntity message) {
        return this.commitLog.put(message);
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
        if (queue.getMaxOffset() == 0) {
            result.setStatus(GetMessageStatus.NO_MESSAGE_IN_QUEUE);
            result.setNextBeginOffset(0);
        } else if (requestOffset < queue.getMinOffset()) {
            result.setStatus(GetMessageStatus.OFFSET_TOO_SMALL);
            result.setNextBeginOffset(queue.getMinOffset());
        } else if (requestOffset == queue.getMaxOffset()) {
            result.setStatus(GetMessageStatus.OFFSET_OVER);
            result.setNextBeginOffset(requestOffset);
        } else if (requestOffset > queue.getMaxOffset()) {
            result.setStatus(GetMessageStatus.OFFSET_OVERFLOW);
            result.setNextBeginOffset(queue.getMaxOffset());
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
