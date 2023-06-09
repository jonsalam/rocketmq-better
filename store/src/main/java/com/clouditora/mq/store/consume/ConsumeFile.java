package com.clouditora.mq.store.consume;

import com.clouditora.mq.store.file.MappedFile;

import java.io.IOException;

public class ConsumeFile extends MappedFile {
    public static final int UNIT_SIZE = 20;

    public ConsumeFile(String path, int fileSize) throws IOException {
        super(path, fileSize);
    }
}
