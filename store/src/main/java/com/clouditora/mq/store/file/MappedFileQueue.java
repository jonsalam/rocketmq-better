package com.clouditora.mq.store.file;

import com.clouditora.mq.store.util.StoreUtil;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

@Slf4j
public class MappedFileQueue<T extends MappedFile> {
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
    protected final CopyOnWriteArrayList<T> files = new CopyOnWriteArrayList<>();

    public MappedFileQueue(String path, int fileSize) {
        this.path = path;
        this.fileSize = fileSize;
    }

    public T getFirstFile() {
        if (this.files.isEmpty()) {
            return null;
        }
        try {
            return this.files.get(0);
        } catch (IndexOutOfBoundsException ignore) {
        } catch (Exception e) {
            log.error("getFirstMappedFile has exception.", e);
        }
        return null;
    }

    /**
     * org.apache.rocketmq.store.MappedFileQueue#getLastMappedFile()
     */
    public T getLastFile() {
        T file = null;
        while (!this.files.isEmpty()) {
            try {
                file = this.files.get(this.files.size() - 1);
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
    public T getCurrentWritingFile(long startOffset) {
        T file = getLastFile();
        if (file == null) {
            // MARK: 被除数 = 除数 x 商 + 余数 ==> 除数 x 商 = 被除数 - 余数
            // MARK: N * mappedFileSize = offset - offset % mappedFileSize
            long offset = startOffset - (startOffset % this.fileSize);
            file = createFileSilence(offset);
            Optional.ofNullable(file).ifPresent(this.files::add);
        } else if (file.isFull()) {
            long offset = file.getFileOffset() + this.fileSize;
            file = createFileSilence(offset);
            Optional.ofNullable(file).ifPresent(this.files::add);
        }
        return file;
    }

    public T getCurrentWritingFile() {
        return getCurrentWritingFile(0L);
    }

    @SuppressWarnings("unchecked")
    private T createFileSilence(long offset) {
        try {
            return (T) createFile(offset);
        } catch (IOException e) {
            log.error("create failed", e);
            return null;
        }
    }

    protected MappedFile createFile(long offset) throws IOException {
        String nextFilePath = "%s/%s".formatted(this.path, StoreUtil.long2String(offset));
        return new MappedFile(nextFilePath, this.fileSize);
    }

    /**
     * org.apache.rocketmq.store.MappedFileQueue#findMappedFileByOffset(long, boolean)
     *
     * @param logOffset 是全局的偏移量, 需要减去第一个映射文件的起始偏移量
     * @implNote TODO: https://issues.apache.org/jira/projects/ROCKETMQ/issues/ROCKETMQ-332
     * 一个线程创建映射文件, 一个线程清除过期的, 另一个线程获取的时候, 可能导致获取的文件是错误的
     * 官方的解决方案是根据下标获取文件, 检查偏移量是否正确, 不正确的话就遍历整个映射文件(2分查找不好么)
     */
    public T slice(long logOffset) {
        T file = sliceWithIndex(logOffset);
        if (file != null) {
            return file;
        }
        file = sliceWithIterator(logOffset);
        return file;
    }

    private T sliceWithIndex(long offset) {
        MappedFile firstFile = getFirstFile();
        if (firstFile == null) {
            return null;
        }
        int index = (int) ((offset - firstFile.getFileOffset()) / fileSize);
        if (index < 0 || index >= files.size()) {
            log.warn("offset not matched, request offset:{}, mappedFiles:{}", offset, files);
        }
        try {
            T file = files.get(index);
            if (calcOffset(offset, file)) {
                return file;
            }
        } catch (Exception ignored) {
        }
        return null;
    }

    private T sliceWithIterator(long offset) {
        for (T file : files) {
            if (calcOffset(offset, file)) {
                return file;
            }
        }
        return null;
    }

    /**
     * 在文件偏移量范围之内
     */
    protected boolean calcOffset(long offset, T file) {
        return offset >= file.getFileOffset() && offset < file.getFileOffset() + fileSize;
    }
}
