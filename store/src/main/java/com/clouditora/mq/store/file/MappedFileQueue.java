package com.clouditora.mq.store.file;

import com.clouditora.mq.store.util.FileUtil;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

@Slf4j
public class MappedFileQueue {
    /**
     * 存储目录
     */
    protected final String path;
    /**
     * 每个映射文件的大小
     */
    protected final int fileSize;
    /**
     * MappedFile 集合
     */
    protected final CopyOnWriteArrayList<MappedFile> files = new CopyOnWriteArrayList<>();

    public MappedFileQueue(String path, int fileSize) {
        this.path = path;
        this.fileSize = fileSize;
    }

    /**
     * org.apache.rocketmq.store.MappedFileQueue#getLastMappedFile()
     */
    public MappedFile getLastFile() {
        MappedFile file = null;
        while (!files.isEmpty()) {
            try {
                file = files.get(files.size() - 1);
                break;
            } catch (IndexOutOfBoundsException e) {
                // mappedFiles 变小了
            } catch (Exception e) {
                log.error("getLastMappedFile has exception.", e);
                break;
            }
        }
        return file;
    }

    /**
     * 当映射文件<b>可用</b>, 直接返回
     * 当映射文件<b>不存在</b>, 创建一个从startOffset开始偏移的文件
     * 当映射文件<b>已经写满</b>, 创建一个新偏移量的文件
     * org.apache.rocketmq.store.MappedFileQueue#getLastMappedFile(long, boolean)
     */
    public MappedFile getCurrentWritingFile(long startOffset) {
        MappedFile file = getLastFile();
        if (file == null) {
            long offset = startOffset - (startOffset % this.fileSize);
            file = createFileSilence(offset);
            Optional.ofNullable(file).ifPresent(files::add);
        } else if (file.isFull()) {
            long offset = file.getFileOffset() + fileSize;
            file = createFileSilence(offset);
            Optional.ofNullable(file).ifPresent(files::add);
        }
        return file;
    }

    public MappedFile getCurrentWritingFile() {
        return getCurrentWritingFile(0L);
    }

    private MappedFile createFileSilence(long offset) {
        try {
            return createFile(offset);
        } catch (IOException e) {
            log.error("create failed", e);
            return null;
        }
    }

    protected MappedFile createFile(long offset) throws IOException {
        String nextFilePath = path + File.separator + FileUtil.long2String(offset);
        return new MappedFile(nextFilePath, fileSize);
    }
}
