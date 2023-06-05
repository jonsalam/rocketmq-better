package com.clouditora.mq.store;

import com.clouditora.mq.common.message.MessageEntity;
import com.clouditora.mq.common.service.AbstractNothingService;
import com.clouditora.mq.store.consume.ConsumeFile;
import com.clouditora.mq.store.consume.ConsumeFileQueue;
import com.clouditora.mq.store.consume.ConsumeFileQueues;
import com.clouditora.mq.store.enums.GetMessageStatus;
import com.clouditora.mq.store.file.GetMessageResult;
import com.clouditora.mq.store.file.MappedFile;
import com.clouditora.mq.store.file.PutResult;
import com.clouditora.mq.store.log.CommitLog;
import com.clouditora.mq.store.log.CommitLogDispatcher;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * @link org.apache.rocketmq.store.DefaultMessageStore
 */
@Slf4j
public class MessageStore extends AbstractNothingService {
    private final MessageStoreConfig storeConfig;
    private final CommitLog commitLog;
    private final ConsumeFileQueues consumeFileQueues;
    private final CommitLogDispatcher dispatcher;

    public MessageStore(MessageStoreConfig storeConfig) {
        this.storeConfig = storeConfig;
        this.commitLog = new CommitLog(storeConfig);
        this.consumeFileQueues = new ConsumeFileQueues(storeConfig);
        this.dispatcher = new CommitLogDispatcher(this.storeConfig, this.commitLog, this.consumeFileQueues);
    }

    @Override
    public String getServiceName() {
        return MessageStore.class.getSimpleName();
    }

    @Override
    public void startup() {
        this.dispatcher.startup();
        super.startup();
    }

    @Override
    public void shutdown() {
        this.dispatcher.shutdown();
        super.shutdown();
    }

    /**
     * org.apache.rocketmq.store.DefaultMessageStore#asyncPutMessage
     */
    public CompletableFuture<PutResult> asyncPut(MessageEntity message) {
        return this.commitLog.asyncPut(message);
    }

    /**
     * @link org.apache.rocketmq.store.DefaultMessageStore#getMessage
     */
    public GetMessageResult get(String group, String topic, int queueId, long requestOffset, int requestNum) {
        GetMessageResult result = new GetMessageResult();
        ConsumeFileQueue queue = this.consumeFileQueues.get(topic, queueId);
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
