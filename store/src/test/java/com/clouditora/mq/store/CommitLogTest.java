package com.clouditora.mq.store;

import com.clouditora.mq.store.file.MappedFile;
import com.clouditora.mq.store.file.MappedFileQueue;
import com.clouditora.mq.store.util.StoreUtil;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CommitLogTest extends AbstractFileTest {

    @Test
    public void putMessage() throws Exception {
        MessageStoreConfig config = new MessageStoreConfig();
        config.setCommitLogFileSize(180);
        config.setRootPath(path);
        CommitLog commitLog = new CommitLog(config);
        MessageEntity message = TestUtil.buildMessage();
        // 一个message的大小是157, 2个会超过一个mappedFile
        for (int i = 0; i < 2; i++) {
            commitLog.putMessage(message);
        }
        MappedFileQueue mappedFileQueue = commitLog.getCommitLogQueue();
        MappedFile mappedFile = mappedFileQueue.getCurrentWritingFile();
        String name = mappedFile.getFile().getName();

        assertEquals(StoreUtil.long2String(180), name);
    }
}
