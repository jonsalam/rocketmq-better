package com.clouditora.mq.store.index;

import com.clouditora.mq.store.file.MappedFileQueue;
import com.clouditora.mq.store.util.StoreUtil;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@Slf4j
public class ConsumeFileQueue extends MappedFileQueue<ConsumeFile> {

    public ConsumeFileQueue(String path, int fileSize) {
        super(path, fileSize);
    }

    @Override
    protected ConsumeFile createFile(long offset) throws IOException {
        String path = "%s/%s".formatted(super.path, StoreUtil.long2String(offset));
        return new ConsumeFile(path, super.fileSize);
    }

    @Override
    public ConsumeFile getCurrentWritingFile(long startOffset) {
        long offset = startOffset * ConsumeFile.UNIT_SIZE;
        return super.getCurrentWritingFile(offset);
    }

    @Override
    public ConsumeFile slice(long logOffset) {
        long offset = logOffset * ConsumeFile.UNIT_SIZE;
        return super.slice(offset);
    }
}
