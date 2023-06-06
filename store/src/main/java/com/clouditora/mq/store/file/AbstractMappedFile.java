package com.clouditora.mq.store.file;

import com.clouditora.mq.common.util.FileUtil;
import com.clouditora.mq.store.util.LibC;
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
import java.util.concurrent.locks.ReentrantLock;

/**
 * @link org.apache.rocketmq.store.ReferenceResource
 */
@Slf4j
public abstract class AbstractMappedFile {
    public static final int OS_PAGE_SIZE = 1024 * 4;
    public static final AtomicLong TOTAL_MAPPED_MEMORY = new AtomicLong(0);
    public static final AtomicInteger TOTAL_MAPPED_FILES = new AtomicInteger(0);

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
    protected final AtomicInteger flushPosition = new AtomicInteger(0);

    /**
     * @link org.apache.rocketmq.store.ReferenceResource#available
     */
    protected volatile boolean available = false;
    /**
     * @link org.apache.rocketmq.store.ReferenceResource#refCount
     */
    protected AtomicLong acquiredCount = new AtomicLong(1);
    private final ReentrantLock lock = new ReentrantLock();

    protected AbstractMappedFile(File file, int fileSize) {
        this.offset = StoreUtil.string2Long(file.getName());
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

    protected AbstractMappedFile(AbstractMappedFile file, int fileSize, MappedByteBuffer mappedByteBuffer) {
        this.offset = file.offset;
        this.file = file.file;
        this.fileSize = fileSize;
        this.fileChannel = file.fileChannel;
        this.mappedByteBuffer = mappedByteBuffer;
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
        long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        {
            int ret = LibC.INSTANCE.mlock(pointer, new NativeLong(this.fileSize));
            log.info("lock {} @{}/{}={}, elapsed={}", this.file, address, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }

        {
            int ret = LibC.INSTANCE.madvise(pointer, new NativeLong(this.fileSize), LibC.MADV_WILLNEED);
            log.info("advise {} @{}/{}={}, elapsed={}", this.file, address, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }
    }

    /**
     * @link org.apache.rocketmq.store.MappedFile#munlock
     */
    public void unlockMappedMemory() {
        long beginTime = System.currentTimeMillis();
        long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        int ret = LibC.INSTANCE.munlock(pointer, new NativeLong(this.fileSize));
        log.info("unlock {} @{}/{}={}, elapsed={}", this.file, address, this.fileSize, ret, System.currentTimeMillis() - beginTime);
    }

    /**
     * @link org.apache.rocketmq.store.MappedFile#destroy
     */
    public void delete() {
        if (this.acquiredCount.get() > 0) {
            log.warn("delete file {} failed: acquired count={}", this.file, this.acquiredCount.get());
            return;
        }
        unmap();
        try {
            this.fileChannel.close();
            boolean delete = this.file.delete();
            log.info("delete file {}={}", this.file, delete);
        } catch (Exception e) {
            log.error("close file {} exception", this.file, e);
        }
        this.available = false;
    }

    /**
     * @link org.apache.rocketmq.store.ReferenceResource#shutdown
     * @link org.apache.rocketmq.store.ReferenceResource#cleanup
     */
    public void unmap() {
        if (!this.available) {
            log.error("unload file {} failed: already unloaded", this.file);
            return;
        }
        try {
            StoreUtil.clean(this.mappedByteBuffer);
            TOTAL_MAPPED_MEMORY.addAndGet(-this.fileSize);
            TOTAL_MAPPED_FILES.decrementAndGet();
        } catch (Exception e) {
            log.error("unload file {} exception", this.file, e);
        }
        this.available = false;
    }

    public void load() {
        this.available = true;
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
        long value = acquiredCount.decrementAndGet();
        if (value > 0) {
            return;
        }
        try {
            this.lock.lock();
            unmap();
        } finally {
            this.lock.unlock();
        }
    }

    public void flush(int pages) {
        boolean needed;
        if (isFull()) {
            needed = true;
        } else {
            int flushPosition1 = this.flushPosition.get();
            int writePosition = getWritePosition();
            if (pages > 0) {
                needed = (writePosition - flushPosition1) / OS_PAGE_SIZE >= pages;
            } else {
                needed = writePosition > flushPosition1;
            }
        }
        if (needed) {
            this.flushPosition.set(getWritePosition());
            force();
        }
    }

    /**
     * @link org.apache.rocketmq.store.MappedFile#warmMappedFile
     */
    public void warmup(FlushType type, int pages) {
        ByteBuffer byteBuffer = getByteBuffer();
        long beginTime = System.currentTimeMillis();
        long flushtime = System.currentTimeMillis();
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

            // prevent gc
            if (j % 1000 == 0) {
                log.info("file warmup to disk: file={}, elapsed={}", this.file, System.currentTimeMillis() - flushtime);
                flushtime = System.currentTimeMillis();
                try {
                    Thread.sleep(0);
                } catch (InterruptedException e) {
                    log.error("file warmup interrupted", e);
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
