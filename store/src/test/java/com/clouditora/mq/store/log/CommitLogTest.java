package com.clouditora.mq.store.log;

import com.clouditora.mq.common.constant.MagicCode;
import com.clouditora.mq.common.message.MessageEntity;
import com.clouditora.mq.store.StoreConfig;
import com.clouditora.mq.store.TestUtil;
import com.clouditora.mq.store.file.AbstractFileTest;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class CommitLogTest extends AbstractFileTest {

    @Test
    void putMessage() {
        int total = 2;
        StoreConfig config = new StoreConfig();
        config.setCommitLogFileSize(1024);
        config.setRootPath(super.path);
        CommitLog commitLog = new CommitLog(config, null);
        MessageEntity message = TestUtil.buildMessage();
        for (int i = 0; i < total; i++) {
            commitLog.put(message);
        }
        int count = 0;
        CommitLogReader iterator = new CommitLogReader(commitLog, 0);
        MessageEntity next;
        while ((next = iterator.read()) != null) {
            if (next.getMagicCode() == MagicCode.MESSAGE) {
                count++;
            }
        }
        assertThat(count).isEqualTo(total);
    }

    @Test
    void recover() {
        putMessage();

        StoreConfig config = new StoreConfig();
        config.setCommitLogFileSize(1024);
        config.setRootPath(super.path);
        CommitLog commitLog = new CommitLog(config, null);
        commitLog.recover(true, null);
    }
}
