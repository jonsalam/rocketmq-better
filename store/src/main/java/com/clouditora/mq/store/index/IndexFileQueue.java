package com.clouditora.mq.store.index;

import com.clouditora.mq.store.MessageStoreConfig;
import com.clouditora.mq.store.file.MappedFileQueue;
import com.clouditora.mq.store.util.FileUtil;

import java.io.File;
import java.io.IOException;

public class IndexFileQueue extends MappedFileQueue<IndexFile> {
    private final MessageStoreConfig config;

    public IndexFileQueue(MessageStoreConfig config) {
        super(config.getIndexPath(), IndexFile.HEADER_SIZE + (IndexFile.SLOT_SIZE * config.getMaxSlotCount()) + (IndexFile.INDEX_SIZE * config.getMaxItemCount()));
        this.config = config;
    }

    @Override
    protected IndexFile createFile(long offset) throws IOException {
        String path = super.path + File.separator + FileUtil.timestamp2String(System.currentTimeMillis());
        return new IndexFile(path, config.getMaxSlotCount(), config.getMaxItemCount());
    }
}
