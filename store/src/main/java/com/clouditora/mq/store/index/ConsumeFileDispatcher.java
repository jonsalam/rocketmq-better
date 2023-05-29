package com.clouditora.mq.store.index;

import com.clouditora.mq.common.MessageConst;
import com.clouditora.mq.store.MessageDispatcher;
import com.clouditora.mq.store.MessageEntity;
import com.clouditora.mq.store.enums.PutStatus;
import com.clouditora.mq.store.exception.PutException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.nio.ByteBuffer;

@Slf4j
public class ConsumeFileDispatcher implements MessageDispatcher {
    private final ConsumeFileMap files;
    private final ByteBuffer byteBuffer;

    public ConsumeFileDispatcher(ConsumeFileMap files) {
        this.files = files;
        this.byteBuffer = ByteBuffer.allocate(ConsumeFile.UNIT_SIZE);
    }

    @Override
    public void dispatch(MessageEntity message) throws Exception {
        ConsumeFileQueue queue = this.files.findConsumeQueue(message.getTopic(), message.getQueueId());
        ConsumeFile file = queue.getCurrentWritingFile(message.getQueueOffset());
        if (file == null) {
            log.error("create file error: topic={}, bornHost={}", message.getTopic(), message.getBornHost());
            throw new PutException(PutStatus.CREATE_MAPPED_FILE_FAILED);
        }

        int tagsCode = getTagsCode(message);
        this.byteBuffer.flip().limit(ConsumeFile.UNIT_SIZE);
        this.byteBuffer.putLong(message.getLogOffset());
        this.byteBuffer.putInt(message.getMessageLength());
        this.byteBuffer.putLong(tagsCode);
        file.append(this.byteBuffer);
    }

    private int getTagsCode(MessageEntity message) {
        String tags = message.getProperties().get(MessageConst.Property.TAGS);
        int tagsCode = 0;
        if (StringUtils.isNotBlank(tags)) {
            tagsCode = tags.hashCode();
        }
        return tagsCode;
    }
}
