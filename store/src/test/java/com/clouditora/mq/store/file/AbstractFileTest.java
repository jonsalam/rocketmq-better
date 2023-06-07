package com.clouditora.mq.store.file;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.platform.commons.util.StringUtils;

import java.io.File;

@Slf4j
public abstract class AbstractFileTest {
    protected String path = "target/unit-test/";

    @AfterEach
    public void init() {
        if (StringUtils.isBlank(path)) {
            return;
        }
        File file = new File(path);
        deleteFile(file);
        MappedFile.TOTAL_MAPPED_MEMORY.set(0);
    }

    public void deleteFile(File file) {
        if (!file.exists()) {
            return;
        }
        if (file.isFile()) {
            boolean result = file.delete();
            log.info("delete {}: {}", file, result);
        } else if (file.isDirectory()) {
            File[] files = file.listFiles();
            if (files == null) {
                return;
            }
            for (File f : files) {
                deleteFile(f);
            }
            boolean result = file.delete();
            log.info("delete {}: {}", file, result);
        }
    }
}
