package com.clouditora.mq.store.log.dispatcher;

import com.clouditora.mq.common.message.MessageEntity;
import com.clouditora.mq.common.service.AbstractLoopedService;
import com.clouditora.mq.store.log.CommitLog;
import com.clouditora.mq.store.log.CommitLogIterator;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.List;

/**
 * org.apache.rocketmq.store.DefaultMessageStore.ReputMessageService
 */
@Slf4j
public class CommitLogDispatcher extends AbstractLoopedService {
    private final CommitLogIterator iterator;
    private final List<MessageDispatcher> dispatchers;

    public CommitLogDispatcher(CommitLog commitLog, MessageDispatcher... dispatchers) {
        this.dispatchers = Arrays.asList(dispatchers);
        this.iterator = new CommitLogIterator(commitLog, 0);
    }

    @Override
    public String getServiceName() {
        return CommitLogDispatcher.class.getSimpleName();
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
        dispatch(message);
    }

    public void dispatch(MessageEntity message) {
        for (MessageDispatcher dispatcher : this.dispatchers) {
            try {
                log.debug("dispatch message: offset={}, topic={}", message.getCommitLogOffset(), message.getTopic());
                dispatcher.dispatch(message);
            } catch (Exception e) {
                log.error("dispatch message exception", e);
            }
        }
    }
}
