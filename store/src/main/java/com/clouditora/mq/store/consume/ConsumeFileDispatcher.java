package com.clouditora.mq.store.consume;

import com.clouditora.mq.common.MessageConst;
import com.clouditora.mq.common.message.MessageEntity;
import com.clouditora.mq.store.log.CommitLogDispatcher;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.nio.ByteBuffer;

/**
 * @link org.apache.rocketmq.store.DefaultMessageStore.CommitLogDispatcherBuildConsumeQueue
 */
@Slf4j
public class ConsumeFileDispatcher implements CommitLogDispatcher {
    private final ConsumeFileQueues files;
    /**
     * @link org.apache.rocketmq.store.ConsumeQueue#byteBufferIndex
     */
    private final ByteBuffer byteBuffer;

    public ConsumeFileDispatcher(ConsumeFileQueues files) {
        this.files = files;
        this.byteBuffer = ByteBuffer.allocate(ConsumeFile.UNIT_SIZE);
    }

    /**
     * @link org.apache.rocketmq.store.ConsumeQueue#putMessagePositionInfo
     */
    @Override
    public void dispatch(MessageEntity message) throws Exception {
        ConsumeFileQueue queue = this.files.get(message.getTopic(), message.getQueueId());
        ConsumeFile file = queue.getOrCreate(message.getQueueOffset());
        if (file == null) {
            return;
        }

        int tagsCode = getMessageHashCode(message);
        this.byteBuffer.flip().limit(ConsumeFile.UNIT_SIZE);
        this.byteBuffer.putLong(message.getCommitLogOffset());
        this.byteBuffer.putInt(message.getMessageLength());
        this.byteBuffer.putLong(tagsCode);
        file.append(this.byteBuffer);
        queue.increaseMaxOffset(message.getMessageLength());
    }

    private int getMessageHashCode(MessageEntity message) {
        String tags = message.getProperties().get(MessageConst.Property.TAGS);
        if (StringUtils.isBlank(tags)) {
            return 0;
        }
        return tags.hashCode();
    }
}
