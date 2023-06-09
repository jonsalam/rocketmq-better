package com.clouditora.mq.store.checkpoint;

import com.clouditora.mq.store.file.MappedFile;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@Slf4j
public class CheckpointFile extends MappedFile {
    public CheckpointFile(String path) throws IOException {
        super(path, 0, 24);
    }
}
