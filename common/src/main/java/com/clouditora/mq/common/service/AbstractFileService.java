package com.clouditora.mq.common.service;

import com.clouditora.mq.common.util.FileUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

@Slf4j
public abstract class AbstractFileService extends AbstractNothingService {
    private final String path;

    public AbstractFileService(String path) {
        this.path = path;
    }

    @Override
    public void startup() {
        String content = read();
        decode(content);
        super.startup();
    }

    private String read() {
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
        save();
    }

    public void save() {
        String content = encode();
        if (StringUtils.isNotBlank(content)) {
            try {
                FileUtil.overwriteFile(content.getBytes(StandardCharsets.UTF_8), this.path);
            } catch (IOException e) {
                log.error("persist file {} exception", this.path, e);
            }
        }
        super.shutdown();
    }

    protected abstract void decode(String content);

    protected abstract String encode();
}
