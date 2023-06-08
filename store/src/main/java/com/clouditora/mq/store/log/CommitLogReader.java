package com.clouditora.mq.store.log;

import com.clouditora.mq.common.message.MessageEntity;
import com.clouditora.mq.common.service.AbstractLoopedService;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.List;

/**
 * org.apache.rocketmq.store.DefaultMessageStore.ReputMessageService
 */
@Slf4j
public class CommitLogReader extends AbstractLoopedService {
    private final CommitLogIterator iterator;
    private final List<CommitLogDispatcher> dispatchers;

    public CommitLogReader(CommitLog commitLog, CommitLogDispatcher... dispatchers) {
        this.dispatchers = Arrays.asList(dispatchers);
        this.iterator = new CommitLogIterator(commitLog, 0);
    }

    @Override
    public String getServiceName() {
        return CommitLogReader.class.getSimpleName();
    }

    @Override
    public void loop() {
        dispatch();
    }

    /**
     * @link org.apache.rocketmq.store.DefaultMessageStore.ReputMessageService#doReput
     */
    private void dispatch() {
        MessageEntity message = this.iterator.next();
        if (message == null) {
            return;
        }
        for (CommitLogDispatcher dispatcher : this.dispatchers) {
            try {
                log.debug("dispatch message: offset={}, topic={}", message.getCommitLogOffset(), message.getTopic());
                dispatcher.dispatch(message);
            } catch (Exception e) {
                log.error("dispatch message error", e);
            }
        }
    }
}
