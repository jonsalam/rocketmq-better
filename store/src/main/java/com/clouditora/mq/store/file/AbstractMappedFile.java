package com.clouditora.mq.store.file;

import com.clouditora.mq.common.util.FileUtil;
import com.clouditora.mq.store.util.LibraryC;
import com.clouditora.mq.store.util.StoreUtil;
import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import sun.nio.ch.DirectBuffer;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @link org.apache.rocketmq.store.ReferenceResource
 */
@Slf4j
public abstract class AbstractMappedFile implements com.clouditora.mq.store.file.File {
    public static final int OS_PAGE_SIZE = 1024 * 4;
    static final AtomicLong TOTAL_MAPPED_MEMORY = new AtomicLong(0);
    static final AtomicInteger TOTAL_MAPPED_FILES = new AtomicInteger(0);

    /**
     * @link org.apache.rocketmq.store.MappedFile#fileFromOffset
     */
    @Getter
    protected final long offset;
    /**
     * @link org.apache.rocketmq.store.MappedFile#file
     */
    @Getter
    protected final File file;
    /**
     * @link org.apache.rocketmq.store.MappedFile#fileSize
     */
    @Getter
    protected final int fileSize;
    /**
     * @link org.apache.rocketmq.store.MappedFile#fileChannel
     */
    protected final FileChannel fileChannel;
    /**
     * @link org.apache.rocketmq.store.MappedFile#mappedByteBuffer
     */
    protected final MappedByteBuffer mappedByteBuffer;
    /**
     * @link org.apache.rocketmq.store.MappedFile#flushedPosition
     */
    protected final AtomicInteger flushPosition = new AtomicInteger(0);
    /**
     * @link org.apache.rocketmq.store.ReferenceResource#cleanupOver
     */
    protected volatile boolean mapped = true;
    /**
     * @link org.apache.rocketmq.store.ReferenceResource#refCount
     */
    protected AtomicLong acquiredCount = new AtomicLong(1);

    protected AbstractMappedFile(File file, long offset, int fileSize) {
        this.offset = offset;
        this.file = file;
        this.fileSize = fileSize;
        try {
            FileUtil.mkdir(this.file.getParent());
            this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
            this.mappedByteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        TOTAL_MAPPED_MEMORY.addAndGet(fileSize);
        TOTAL_MAPPED_FILES.incrementAndGet();
    }

    protected AbstractMappedFile(File file, int fileSize) {
        this(file, StoreUtil.string2Long(file.getName()), fileSize);
    }

    protected AbstractMappedFile(AbstractMappedFile file, int fileSize, MappedByteBuffer mappedByteBuffer) {
        this.offset = file.offset;
        this.file = file.file;
        this.fileSize = fileSize;
        this.fileChannel = file.fileChannel;
        this.mappedByteBuffer = mappedByteBuffer;
        this.acquiredCount.set(file.acquiredCount.get());
    }

    @Override
    public String toString() {
        return this.file.getPath() + "@" + getWritePosition();
    }

    public void setWritePosition(int position) {
        this.mappedByteBuffer.position(position);
    }

    public int getWritePosition() {
        return this.mappedByteBuffer.position();
    }

    public void setFlushPosition(int position) {
        this.flushPosition.set(position);
    }

    public int getFlushPosition() {
        return this.flushPosition.get();
    }

    public MappedByteBuffer getByteBuffer() {
        return this.mappedByteBuffer;
    }

    /**
     * @link org.apache.rocketmq.store.MappedFile#isFull
     */
    public boolean isFull() {
        return getWritePosition() >= this.fileSize;
    }

    protected void force() {
        this.mappedByteBuffer.force();
    }

    /**
     * @link org.apache.rocketmq.store.MappedFile#mlock
     */
    public void lockMappedMemory() {
        long beginTime = System.currentTimeMillis();
        long address = ((DirectBuffer) this.mappedByteBuffer).address();
        Pointer pointer = new Pointer(address);
        {
            int ret = LibraryC.INSTANCE.mlock(pointer, new NativeLong(this.fileSize));
            log.info("lock {} @{}/{}={}, elapsed={}", this.file, address, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }

        {
            int ret = LibraryC.INSTANCE.madvise(pointer, new NativeLong(this.fileSize), LibraryC.MADV_WILLNEED);
            log.info("advise {} @{}/{}={}, elapsed={}", this.file, address, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }
    }

    /**
     * @link org.apache.rocketmq.store.MappedFile#munlock
     */
    public void unlockMappedMemory() {
        long beginTime = System.currentTimeMillis();
        long address = ((DirectBuffer) this.mappedByteBuffer).address();
        Pointer pointer = new Pointer(address);
        int ret = LibraryC.INSTANCE.munlock(pointer, new NativeLong(this.fileSize));
        log.info("unlock {} @{}/{}={}, elapsed={}", this.file, address, this.fileSize, ret, System.currentTimeMillis() - beginTime);
    }

    /**
     * @link org.apache.rocketmq.store.ReferenceResource#hold
     */
    public void acquire() {
        this.acquiredCount.incrementAndGet();
    }

    /**
     * @link org.apache.rocketmq.store.ReferenceResource#release
     */
    public void release() {
        long value = this.acquiredCount.decrementAndGet();
        if (value == 0) {
            unmap();
        }
    }

    @Override
    public void map() {
        if (!this.mapped) {
            throw new IllegalStateException();
        }
    }

    /**
     * @link org.apache.rocketmq.store.ReferenceResource#cleanup
     */
    @Override
    public void unmap() {
        if (!this.mapped && this.acquiredCount.get() > 0) {
            return;
        }
        this.mapped = false;
        this.acquiredCount.set(0);
        try {
            StoreUtil.clean(this.mappedByteBuffer);
            TOTAL_MAPPED_MEMORY.addAndGet(-this.fileSize);
            TOTAL_MAPPED_FILES.decrementAndGet();
            log.info("unmapped file {}", this.file);
        } catch (Exception e) {
            log.error("unmapped file {} exception", this.file, e);
        }
    }

    /**
     * @link org.apache.rocketmq.store.MappedFile#destroy
     */
    @Override
    public void delete() {
        release();
        if (this.mapped) {
            log.warn("delete mapped file {} failed: unmapped", this.file);
            return;
        }
        try {
            this.fileChannel.close();
            boolean delete = this.file.delete();
            log.info("delete mapped file {}: {}", this.file, delete);
        } catch (Exception e) {
            log.error("delete mapped file {} exception", this.file, e);
        }
    }

    @Override
    public long flush(int pages) {
        int flushPosition = getFlushPosition();
        int writePosition = getWritePosition();
        boolean needed;
        if (isFull()) {
            needed = true;
        } else {
            if (pages > 0) {
                needed = (writePosition - flushPosition) / OS_PAGE_SIZE >= pages;
            } else {
                needed = writePosition > flushPosition;
            }
        }
        if (needed) {
            try {
                acquire();
                force();
                setFlushPosition(writePosition);
                return writePosition;
            } catch (Exception e) {
                log.error("flush file {} exception", this.file, e);
            } finally {
                release();
            }
        }
        return flushPosition;
    }

    /**
     * @link org.apache.rocketmq.store.MappedFile#warmMappedFile
     */
    public void warmup(FlushType type, int pages) {
        ByteBuffer byteBuffer = getByteBuffer();
        long beginTime = System.currentTimeMillis();
        int flushIndex = 0;
        for (int index = 0, j = 0; index < this.fileSize; index += OS_PAGE_SIZE, j++) {
            byteBuffer.put(index, (byte) 0);
            // force flush when flush disk type is sync
            if (type == FlushType.SYNC_FLUSH) {
                if ((index - flushIndex) / OS_PAGE_SIZE >= pages) {
                    flushIndex = index;
                    force();
                }
            }
        }

        // force flush when prepare load finished
        if (type == FlushType.SYNC_FLUSH) {
            log.info("file warmup to disk: file={}, elapsed={}", this.file, System.currentTimeMillis() - beginTime);
            force();
        }
        log.info("file warmup done: file={}, elapsed={}", this.file, System.currentTimeMillis() - beginTime);
        lockMappedMemory();
    }

}
