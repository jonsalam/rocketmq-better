package com.clouditora.mq.store.file;

import com.clouditora.mq.store.util.StoreUtil;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @link org.apache.rocketmq.store.MappedFileQueue
 */
@Slf4j
public class MappedFileQueue<T extends MappedFile> {
    /**
     * 存储目录
     */
    protected final String dir;
    /**
     * 每个映射文件的大小
     */
    protected final int fileSize;
    /**
     * MappedFile 集合
     */
    protected final CopyOnWriteArrayList<T> files = new CopyOnWriteArrayList<>();

    public MappedFileQueue(String dir, int fileSize) {
        this.dir = dir;
        this.fileSize = fileSize;
    }

    public void load() {
        File dir = new File(this.dir);
        File[] files = dir.listFiles();
        if (files != null) {
            loadFile(Arrays.asList(files));
        }
    }

    public void loadFile(List<File> files) {
        files.sort(Comparator.comparing(File::getName));
        for (File file : files) {
            if (file.length() != this.fileSize) {
                log.error("load file {} failed: file size not matched message store config value", file);
                return;
            }
            try {
                T mappedFile = createSilence(StoreUtil.string2Long(file.getName()));
                mappedFile.setWritePosition(this.fileSize);
                mappedFile.setFlushPosition(this.fileSize);
                this.files.add(mappedFile);
                log.error("load file {} success", mappedFile);
            } catch (Exception e) {
                log.error("load file {} exception", file, e);
                return;
            }
        }
    }

    /**
     * @link org.apache.rocketmq.store.MappedFileQueue#getFirstMappedFile
     */
    public T getFirstFile() {
        if (this.files.isEmpty()) {
            return null;
        }
        try {
            return this.files.get(0);
        } catch (IndexOutOfBoundsException ignored) {
        } catch (Exception e) {
            log.error("get first file exception.", e);
        }
        return null;
    }

    /**
     * @link org.apache.rocketmq.store.MappedFileQueue#getLastMappedFile
     */
    public T getLastFile() {
        while (!this.files.isEmpty()) {
            try {
                return this.files.get(this.files.size() - 1);
            } catch (IndexOutOfBoundsException ignored) {
            } catch (Exception e) {
                log.error("get last file exception", e);
                return null;
            }
        }
        return null;
    }

    /**
     * 当映射文件<b>不存在</b>, 根据offset创建新文件
     * 当映射文件<b>已经写满</b>, 创建新文件
     * 否则返回队列中偏移量最大的文件
     *
     * @link org.apache.rocketmq.store.MappedFileQueue#getLastMappedFile(long, boolean)
     */
    public T getOrCreate(long offset) {
        T file = getLastFile();
        if (file == null) {
            // 被除数 = 除数 x 商 + 余数 ==> 除数 x 商 = 被除数 - 余数
            long startOffset = offset - (offset % this.fileSize);
            return createSilence(startOffset);
        } else if (file.isFull()) {
            // 文件写满了
            long startOffset = file.getOffset() + this.fileSize;
            return createSilence(startOffset);
        }
        return file;
    }

    public T getOrCreate() {
        return getOrCreate(0L);
    }

    @SuppressWarnings("unchecked")
    protected T createSilence(long offset) {
        try {
            T file = (T) create(offset);
            this.files.add(file);
            return file;
        } catch (IOException e) {
            log.error("create file exception", e);
            return null;
        }
    }

    /**
     * @link org.apache.rocketmq.store.MappedFileQueue#doCreateMappedFile
     */
    protected MappedFile create(long offset) throws IOException {
        try {
            String newPath = "%s/%s".formatted(this.dir, StoreUtil.long2String(offset));
            return new MappedFile(newPath, this.fileSize);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @param offset 是全局的偏移量, 需要减去第一个映射文件的起始偏移量
     * @link org.apache.rocketmq.store.MappedFileQueue#findMappedFileByOffset(long, boolean)
     * @implNote TODO: https://issues.apache.org/jira/projects/ROCKETMQ/issues/ROCKETMQ-332
     * 一个线程创建映射文件, 一个线程清除过期的, 另一个线程获取的时候, 可能导致获取的文件是错误的
     * 官方的解决方案是根据下标获取文件, 检查偏移量是否正确, 不正确的话就遍历整个映射文件(2分查找不好么)
     */
    public T slice(long offset) {
        T file = sliceWithIndex(offset);
        if (file != null) {
            return file;
        }
        return sliceWithIterator(offset);
    }

    private T sliceWithIndex(long offset) {
        MappedFile firstFile = getFirstFile();
        MappedFile lastFile = getLastFile();
        if (firstFile == null || lastFile == null) {
            return null;
        }
        if (offset < firstFile.getOffset() || offset >= lastFile.getOffset() + this.fileSize) {
            log.warn("offset not matched: offset:{}, star:{}, end:{}", offset, firstFile.getOffset(), lastFile.getOffset() + this.fileSize);
            return null;
        }
        try {
            long index = (offset - firstFile.getOffset()) / this.fileSize;
            T file = this.files.get((int) index);
            if (offsetMatched(file, offset)) {
                return file;
            }
        } catch (Exception ignored) {
        }
        return null;
    }

    private T sliceWithIterator(long offset) {
        for (T file : this.files) {
            if (offsetMatched(file, offset)) {
                return file;
            }
        }
        return null;
    }

    /**
     * 在文件偏移量范围之内
     */
    protected boolean offsetMatched(T file, long offset) {
        if (file == null) {
            return false;
        }
        return file.getOffset() <= offset && offset < file.getOffset() + this.fileSize;
    }
}
