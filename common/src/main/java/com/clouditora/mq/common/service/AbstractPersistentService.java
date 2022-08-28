package com.clouditora.mq.common.service;

import com.clouditora.mq.common.util.FileUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

@Slf4j
public abstract class AbstractPersistentService extends AbstractNothingService {
    private final String path;

    public AbstractPersistentService(String path) {
        this.path = path;
    }

    @Override
    public void startup() {
        String content = readFile();
        load(content);
        super.startup();
    }

    private String readFile() {
        String path = this.path;
        try {
            String content = FileUtil.file2String(path);
            if (StringUtils.isBlank(content)) {
                path = this.path + ".bak";
                content = FileUtil.file2String(path);
            }
            return content;
        } catch (Exception e) {
            log.error("load file {} exception", path, e);
        }
        return null;
    }

    @Override
    public void shutdown() {
        persist();
    }

    protected void persist() {
        String content = unload();
        if (StringUtils.isNoneBlank(content)) {
            String path = this.path;
            try {
                FileUtil.overwriteFile(content.getBytes(StandardCharsets.UTF_8), path);
            } catch (IOException e) {
                log.error("persist file {} exception", path, e);
            }
        }
        super.shutdown();
    }

    protected abstract void load(String content);

    protected abstract String unload();
}
