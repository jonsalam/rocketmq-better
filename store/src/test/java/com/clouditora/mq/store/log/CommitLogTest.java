package com.clouditora.mq.store.log;

import com.clouditora.mq.common.constant.MagicCode;
import com.clouditora.mq.common.message.MessageEntity;
import com.clouditora.mq.store.MessageStoreConfig;
import com.clouditora.mq.store.TestUtil;
import com.clouditora.mq.store.file.AbstractFileTest;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class CommitLogTest extends AbstractFileTest {

    @Test
    void putMessage() {
        int total = 2;
        MessageStoreConfig config = new MessageStoreConfig();
        config.setCommitLogFileSize(1024);
        config.setRootPath(super.path);
        CommitLog commitLog = new CommitLog(config);
        MessageEntity message = TestUtil.buildMessage();
        for (int i = 0; i < total; i++) {
            commitLog.put(message);
        }
        int count = 0;
        CommitLogIterator iterator = new CommitLogIterator(commitLog, 0);
        while (iterator.hasNext()) {
            MessageEntity next = iterator.next();
            if (next != null && next.getMagicCode() == MagicCode.MESSAGE) {
                count++;
            }
        }
        assertThat(count).isEqualTo(total);
    }

    @Test
    void recover() {
        putMessage();

        MessageStoreConfig config = new MessageStoreConfig();
        config.setCommitLogFileSize(1024);
        config.setRootPath(super.path);
        CommitLog commitLog = new CommitLog(config);
        commitLog.recover();
    }
}
