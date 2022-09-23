package com.clouditora.mq.store.index;

import com.clouditora.mq.store.file.MappedFile;

import java.io.IOException;

public class ConsumeFile extends MappedFile {
    public static final int UNIT_SIZE = 20;

    public ConsumeFile(String fileName, int fileSize) throws IOException {
        super(fileName, fileSize);
    }
}
