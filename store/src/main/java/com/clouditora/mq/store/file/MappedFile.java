package com.clouditora.mq.store.file;

import com.clouditora.mq.store.enums.PutStatus;
import com.clouditora.mq.store.exception.PutException;
import com.clouditora.mq.store.util.FileUtil;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * mappedByteBuffer的position不改变, 手动额外维护读写position
 */
@Slf4j
public class MappedFile {
    /**
     * 如果文件没有写满, 值与mappedByteBuffer的position相同
     * 如果文件已经写满, 值与fileSize相同
     */
    protected final AtomicInteger writePosition = new AtomicInteger(0);

    @Getter
    protected final File file;
    @Getter
    protected final int fileSize;
    /**
     * org.apache.rocketmq.store.MappedFile#fileFromOffset
     */
    @Getter
    protected final long fileOffset;
    protected final FileChannel fileChannel;
    protected final ByteBuffer mappedByteBuffer;

    public MappedFile(String fileName, int fileSize) throws IOException {
        this.fileSize = fileSize;
        this.file = new File(fileName);
        this.fileOffset = FileUtil.string2Long(this.file.getName());
        FileUtil.mkdir(this.file.getParent());
        this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
        this.mappedByteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
    }

    @Override
    public String toString() {
        return file.getPath() + "#" + writePosition.get();
    }

    /**
     * 写的位置 >= 文件大小
     */
    public boolean isFull() {
        return writePosition.get() >= fileSize;
    }

    public int getWritePosition() {
        return writePosition.get();
    }

    public ByteBuffer getByteBuffer() {
        return mappedByteBuffer.slice();
    }

    public void write(ByteBuffer byteBuffer, int length) throws PutException {
        if (isFull()) {
            // MappedFileQueue#getCurrentWritingFile 已经保证写位置不会超过文件大小
            log.error("file is full: file={}, wrotePosition={} fileSize={}", file, writePosition.get(), fileSize);
            throw new PutException(PutStatus.UNKNOWN_ERROR);
        }
        mappedByteBuffer.slice().put(byteBuffer.array(), 0, byteBuffer.limit());
        this.writePosition.addAndGet(length);
    }

    public void write(ByteBuffer byteBuffer) throws PutException {
        this.write(byteBuffer, byteBuffer.limit());
    }
}
