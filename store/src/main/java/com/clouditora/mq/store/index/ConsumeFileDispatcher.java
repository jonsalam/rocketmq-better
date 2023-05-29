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
    private final ConsumeFileMap map;
    private final ByteBuffer byteBuffer;

    public ConsumeFileDispatcher(ConsumeFileMap map) {
        this.map = map;
        this.byteBuffer = ByteBuffer.allocate(ConsumeFile.UNIT_SIZE);
    }

    @Override
    public void dispatch(MessageEntity message) throws Exception {
        ConsumeFileQueue queue = map.findConsumeQueue(message.getTopic(), message.getQueueId());
        ConsumeFile file = queue.getCurrentWritingFile(message.getQueueOffset());
        if (file == null) {
            log.error("create file error: topic={}, bornHost={}", message.getTopic(), message.getBornHost());
            throw new PutException(PutStatus.CREATE_MAPPED_FILE_FAILED);
        }

        int tagsCode = getTagsCode(message);
        byteBuffer.flip().limit(ConsumeFile.UNIT_SIZE);
        byteBuffer.putLong(message.getLogOffset());
        byteBuffer.putInt(message.getMessageLength());
        byteBuffer.putLong(tagsCode);
        file.append(byteBuffer);
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
