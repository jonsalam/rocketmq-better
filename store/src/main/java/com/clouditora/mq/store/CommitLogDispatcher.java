package com.clouditora.mq.store;

import com.clouditora.mq.common.MessageConst;
import com.clouditora.mq.store.file.MappedFile;
import com.clouditora.mq.store.index.ConsumeFileDispatcher;
import com.clouditora.mq.store.index.ConsumeFileMap;
import com.clouditora.mq.store.index.IndexFileDispatcher;
import com.clouditora.mq.store.serializer.ByteBufferDeserializer;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * org.apache.rocketmq.store.DefaultMessageStore.ReputMessageService
 */
@Slf4j
public class CommitLogDispatcher implements Runnable {
    private final CommitLog commitLog;
    private final ByteBufferDeserializer deserializer;
    private final List<MessageDispatcher> dispatchers;
    private long offset;

    public CommitLogDispatcher(MessageStoreConfig config, CommitLog commitLog, ConsumeFileMap map) {
        this.commitLog = commitLog;
        this.deserializer = new ByteBufferDeserializer();
        ConsumeFileDispatcher consumeFileDispatcher = new ConsumeFileDispatcher(map);
        IndexFileDispatcher indexFileDispatcher = new IndexFileDispatcher(config);
        this.dispatchers = List.of(consumeFileDispatcher, indexFileDispatcher);
    }

    @Override
    public void run() {
        while (true) {
            try {
                Thread.sleep(1);
                dispatch();
            } catch (InterruptedException e) {
                log.error("MessageDispatcher has exception", e);
            }
        }
    }

    private void dispatch() {
        MappedFile file = this.commitLog.slice(offset);
        if (file == null) {
            return;
        }
        offset = file.getFileOffset();
        ByteBuffer byteBuffer = file.getByteBuffer();
        for (long position = 0; position < file.getWritePosition(); ) {
            // TODO 消息可能反序列化失败
            MessageEntity message = this.deserializer.deserialize(byteBuffer);
            if (message.getMagicCode() == MessageConst.MagicCode.BLANK) {
                // 空白消息, 该读取下一个文件的消息了
                // MARK: 被除数 = 除数 x 商 + 余数 ==> 除数 x 商 = 被除数 - 余数
                // MARK: N * mappedFileSize = offset - offset % mappedFileSize
                offset = offset + this.commitLog.getFileSize() - offset % this.commitLog.getFileSize();
                break;
            } else {
                offset += message.getMessageLength();
                position += message.getMessageLength();
                for (MessageDispatcher dispatcher : this.dispatchers) {
                    try {
                        log.debug("{}: offset={}, topic={}", dispatcher.getClass().getSimpleName(), offset, message.getTopic());
                        dispatcher.dispatch(message);
                    } catch (Exception e) {
                        log.error("dispatch error", e);
                    }
                }
            }
        }
    }
}
