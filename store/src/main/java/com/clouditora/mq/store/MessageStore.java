package com.clouditora.mq.store;

import com.clouditora.mq.store.file.MappedFile;
import com.clouditora.mq.store.file.MappedFileHolder;
import com.clouditora.mq.store.file.PutResult;
import com.clouditora.mq.store.index.ConsumeFile;
import com.clouditora.mq.store.index.ConsumeFileMap;
import com.clouditora.mq.store.index.ConsumeFileQueue;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

/**
 * @link org.apache.rocketmq.store.DefaultMessageStore
 */
@Slf4j
public class MessageStore {
    private final MessageStoreConfig storeConfig;
    private final CommitLog commitLog;
    private final ConsumeFileMap consumeFileMap;
    private final Thread dispatcher;

    public MessageStore(MessageStoreConfig storeConfig) {
        this.storeConfig = storeConfig;
        this.commitLog = new CommitLog(storeConfig);
        this.consumeFileMap = new ConsumeFileMap(storeConfig);
        this.dispatcher = new Thread(new CommitLogDispatcher(this.storeConfig, this.commitLog, this.consumeFileMap));
    }

    public void start() {
        this.dispatcher.start();
    }

    /**
     * org.apache.rocketmq.store.DefaultMessageStore#asyncPutMessage
     */
    public CompletableFuture<PutResult> asyncPut(MessageEntity message) {
        return commitLog.asyncPut(message);
    }

    public MappedFileHolder get(String topic, int queueId, long offset, int maxNum) {
        ConsumeFileQueue queue = consumeFileMap.findConsumeQueue(topic, queueId);
        if (queue == null) {
            return null;
        }
        ConsumeFile consumeFile = queue.slice(offset);
        if (consumeFile == null) {
            return null;
        }
        MappedFileHolder result = new MappedFileHolder();
        for (int position = 0; position < consumeFile.getWritePosition() && position < maxNum * ConsumeFile.UNIT_SIZE; position += ConsumeFile.UNIT_SIZE) {
            ByteBuffer byteBuffer = consumeFile.getByteBuffer();
            long logOffset = byteBuffer.getLong();
            int messageSize = byteBuffer.getInt();

            MappedFile logFile = commitLog.slice(logOffset, messageSize);
            if (logFile == null) {
                continue;
            }
            result.addMappedFile(logFile);

        }
        return result;
    }
}
