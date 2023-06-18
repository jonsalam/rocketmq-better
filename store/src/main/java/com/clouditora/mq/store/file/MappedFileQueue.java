package com.clouditora.mq.store.file;

import com.clouditora.mq.common.util.NumberUtil;
import com.clouditora.mq.store.util.StoreUtil;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

/**
 * @link org.apache.rocketmq.store.MappedFileQueue
 */
@Slf4j
public class MappedFileQueue<T extends MappedFile> implements com.clouditora.mq.store.file.File {
    /**
     * @link org.apache.rocketmq.store.MappedFileQueue#storePath
     */
    protected final File dir;
    /**
     * @link org.apache.rocketmq.store.MappedFileQueue#mappedFileSize
     */
    protected final int fileSize;
    /**
     * @link org.apache.rocketmq.store.MappedFileQueue#mappedFiles
     */
    @Getter
    protected final List<T> files = new CopyOnWriteArrayList<>();
    /**
     * @link org.apache.rocketmq.store.MappedFileQueue#flushedWhere
     */
    @Getter
    @Setter
    protected long flushOffset = 0;

    public MappedFileQueue(File dir, int fileSize) {
        this.dir = dir;
        if (this.dir.exists() && !this.dir.isDirectory()) {
            throw new IllegalStateException("create mapped file exception: %s is not directory".formatted(dir));
        }
        this.fileSize = fileSize;
    }

    public MappedFileQueue(String dir, int fileSize) {
        this(new File(dir), fileSize);
    }

    @Override
    public String toString() {
        return "%s@%s".formatted(this.dir.toString(), this.flushOffset);
    }

    /**
     * @link org.apache.rocketmq.store.MappedFileQueue#load
     */
    @Override
    public void map() {
        File[] files = this.dir.listFiles();
        if (files == null) {
            return;
        }
        List<File> list = Arrays.stream(files)
                .filter(e -> NumberUtil.isNumber(e.getName()))
                .sorted(Comparator.comparing(File::getName))
                .collect(Collectors.toList());
        map(list);
    }

    /**
     * @link org.apache.rocketmq.store.MappedFileQueue#doLoad
     */
    private void map(List<File> files) {
        for (File file : files) {
            if (file.length() != this.fileSize) {
                log.error("map file {} failed: file size not matched message store config value", file);
                throw new IllegalStateException("illegal mapped file size");
            }
            try {
                T mappedFile = createSilence(StoreUtil.string2Long(file.getName()));
                mappedFile.map();
                log.info("map file {} success", mappedFile);
            } catch (Exception e) {
                log.error("map file {} exception", file, e);
                return;
            }
        }
    }

    /**
     * @link org.apache.rocketmq.store.MappedFileQueue#shutdown
     */
    @Override
    public void unmap() {
        for (T file : this.files) {
            file.unmap();
        }
    }

    /**
     * @link org.apache.rocketmq.store.MappedFileQueue#destroy
     */
    @Override
    public void delete() {
        for (T file : this.files) {
            file.delete();
        }
        boolean delete = this.dir.delete();
        log.info("delete mapped files dir {}: {}", this.dir, delete);
    }

    /**
     * @param offset 删除大于offset的文件
     * @link org.apache.rocketmq.store.MappedFileQueue#truncateDirtyFiles
     * @link org.apache.rocketmq.store.MappedFileQueue#deleteExpiredFile
     */
    public void delete(long offset) {
        Iterator<T> iterator = this.files.iterator();
        while (iterator.hasNext()) {
            T file = iterator.next();
            if (file.getOffset() > offset) {
                file.unmap();
                iterator.remove();
                log.info("delete file: {}", file);
            }
        }
    }

    @Override
    public long flush(int pages) {
        T file = slice(this.flushOffset);
        if (file == null) {
            file = getFirst();
        }
        if (file != null) {
            long flushPosition = file.flush(pages);
            this.flushOffset = file.getOffset() + flushPosition;
        }
        return this.flushOffset;
    }

    /**
     * @link org.apache.rocketmq.store.MappedFileQueue#getMappedMemorySize
     */
    public long getMappedMemorySize() {
        return AbstractMappedFile.TOTAL_MAPPED_MEMORY.get();
    }

    /**
     * @link org.apache.rocketmq.store.MappedFileQueue#getFirstMappedFile
     */
    public T getFirst() {
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
    public T getLast() {
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
        T file = getLast();
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
        MappedFile firstFile = getFirst();
        MappedFile lastFile = getLast();
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

    /**
     * @link org.apache.rocketmq.store.MappedFileQueue#getMaxOffset
     * @link org.apache.rocketmq.store.MappedFileQueue#getMaxWrotePosition
     */
    public long getMaxWriteOffset() {
        MappedFile file = getLast();
        if (file != null) {
            return file.getOffset() + file.getWritePosition();
        }
        return 0;
    }
}
