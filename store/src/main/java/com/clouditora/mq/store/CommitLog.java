package com.clouditora.mq.store;

import com.clouditora.mq.store.enums.PutStatus;
import com.clouditora.mq.store.exception.PutException;
import com.clouditora.mq.store.file.MappedFile;
import com.clouditora.mq.store.file.MappedFileQueue;
import com.clouditora.mq.store.serializer.ByteBufferSerializer;
import com.clouditora.mq.store.serializer.SerializeException;
import com.clouditora.mq.store.serializer.serializer.EndOfFileException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;

@Slf4j
public class CommitLog {
    @Getter
    private final int fileSize;
    @Getter
    private final MappedFileQueue<MappedFile> commitLogQueue;
    private final ThreadLocal<ByteBufferSerializer> serializerThreadLocal;

    public CommitLog(MessageStoreConfig config) {
        this.fileSize = config.getCommitLogFileSize();
        this.commitLogQueue = new MappedFileQueue<>(config.getCommitLogPath(), config.getCommitLogFileSize());
        this.serializerThreadLocal = ThreadLocal.withInitial(ByteBufferSerializer::new);
    }

    /**
     * org.apache.rocketmq.store.CommitLog#asyncPutMessage
     */
    public void putMessage(MessageEntity message) throws PutException {
        MappedFile file = commitLogQueue.getCurrentWritingFile();
        if (file == null) {
            log.error("create file error: topic={}, bornHost={}", message.getTopic(), message.getBornHost());
            throw new PutException(PutStatus.CREATE_MAPPED_FILE_FAILED);
        }
        try {
            // 当前文件写指针
            int writePosition = file.getWritePosition();
            // 物理偏移量
            long physicalOffset = file.getFileOffset() + writePosition;
            // 当前文件剩余空间
            int remainLength = file.getFileSize() - writePosition;
            ByteBuffer byteBuffer = serializerThreadLocal.get().serialize(physicalOffset, remainLength, message);
            file.write(byteBuffer);
        } catch (EndOfFileException e) {
            log.debug("end of file: file={}, messageLength={}", file, e.getMessageLength());
            // 剩余空间不够, 当前文件写满, 消息写入下一个文件
            file.write(e.getByteBuffer(), e.getRemainLength());
            this.putMessage(message);
        } catch (SerializeException e) {
            throw new PutException(PutStatus.MESSAGE_ILLEGAL);
        } catch (Exception e) {
            log.error("unknown error", e);
            throw new PutException(PutStatus.UNKNOWN_ERROR);
        }
    }

    public MappedFile slice(long logOffset, int length) {
        MappedFile file = commitLogQueue.slice(logOffset);
        if (file == null) {
            return null;
        }
        int offset = (int) (logOffset % fileSize);
        return file.slice(offset, length);
    }

    public MappedFile slice(long logOffset) {
        return slice(logOffset, 0);
    }
}
