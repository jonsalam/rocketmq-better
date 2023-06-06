package com.clouditora.mq.store.log;

import com.clouditora.mq.common.MessageConst;
import com.clouditora.mq.common.message.MessageEntity;
import com.clouditora.mq.common.service.AbstractLoopedService;
import com.clouditora.mq.store.MessageStoreConfig;
import com.clouditora.mq.store.consume.ConsumeFileDispatcher;
import com.clouditora.mq.store.consume.ConsumeFileQueues;
import com.clouditora.mq.store.file.MappedFile;
import com.clouditora.mq.store.index.IndexFileDispatcher;
import com.clouditora.mq.store.serialize.ByteBufferDeserializer;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * org.apache.rocketmq.store.DefaultMessageStore.ReputMessageService
 */
@Slf4j
public class CommitLogDispatcher extends AbstractLoopedService {
    private final CommitLog commitLog;
    private final ByteBufferDeserializer deserializer;
    private final List<MessageDispatcher> dispatchers;
    /**
     * @link org.apache.rocketmq.store.DefaultMessageStore.ReputMessageService#reputFromOffset
     */
    private long offset;

    public CommitLogDispatcher(MessageStoreConfig config, CommitLog commitLog, ConsumeFileQueues map) {
        this.commitLog = commitLog;
        this.deserializer = new ByteBufferDeserializer();
        ConsumeFileDispatcher consumeFileDispatcher = new ConsumeFileDispatcher(map);
        IndexFileDispatcher indexFileDispatcher = new IndexFileDispatcher(config);
        this.dispatchers = List.of(consumeFileDispatcher, indexFileDispatcher);
    }

    @Override
    public String getServiceName() {
        return CommitLogDispatcher.class.getSimpleName();
    }

    @Override
    public void loop() {
        try {
            Thread.sleep(1);
            dispatch();
        } catch (Exception e) {
            log.warn("dispatch message exception. ", e);
        }
    }

    /**
     * @link org.apache.rocketmq.store.DefaultMessageStore.ReputMessageService#doReput
     */
    private void dispatch() {
        MappedFile file = this.commitLog.slice(this.offset);
        if (file == null) {
            return;
        }
        this.offset = file.getOffset();
        ByteBuffer byteBuffer = file.getByteBuffer();
        for (long position = 0; position < file.getWritePosition(); ) {
            // TODO 消息可能反序列化失败
            MessageEntity message = this.deserializer.deserialize(byteBuffer);
            if (message.getMagicCode() == MessageConst.MagicCode.BLANK) {
                // 空白消息, 该读取下一个文件的消息了
                this.offset = this.offset + this.commitLog.getFileSize() - this.offset % this.commitLog.getFileSize();
                break;
            } else {
                this.offset += message.getMessageLength();
                position += message.getMessageLength();
                for (MessageDispatcher dispatcher : this.dispatchers) {
                    try {
                        log.debug("{}: offset={}, topic={}", dispatcher.getClass().getSimpleName(), this.offset, message.getTopic());
                        dispatcher.dispatch(message);
                    } catch (Exception e) {
                        log.error("dispatch error", e);
                    }
                }
            }
        }
    }
}
