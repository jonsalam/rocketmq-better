package com.clouditora.mq.store;

import lombok.Data;

import java.io.File;

@Data
public class MessageStoreConfig {
    private String rootPath = System.getProperty("user.home") + File.separator + "raptor-mq" + File.separator + "store";
    // CommitLog file size, default is 1G
    private int commitLogFileSize = 1024 * 1024 * 1024;

    public String getCommitLogPath() {
        return rootPath + File.separator + "commitlog";
    }
}
