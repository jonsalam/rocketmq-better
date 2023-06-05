package com.clouditora.mq.store.file;

import com.clouditora.mq.common.util.FileUtil;
import com.clouditora.mq.store.enums.PutStatus;
import com.clouditora.mq.store.exception.PutException;
import com.clouditora.mq.store.util.StoreUtil;
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
    protected final AtomicInteger writePosition;
    @Getter
    protected final File file;
    @Getter
    protected final int fileSize;
    /**
     * org.apache.rocketmq.store.MappedFile#fileFromOffset
     */
    @Getter
    protected final long startOffset;
    protected final FileChannel fileChannel;
    protected final ByteBuffer mappedByteBuffer;

    public MappedFile(String fileName, int fileSize) throws IOException {
        this.writePosition = new AtomicInteger(0);
        this.fileSize = fileSize;
        this.file = new File(fileName);
        this.startOffset = StoreUtil.string2Long(this.file.getName());
        FileUtil.mkdir(this.file.getParent());
        this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
        this.mappedByteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
    }

    /**
     * slice用
     * FIXME: fileSize
     */
    public MappedFile(MappedFile mappedFile, long offset, int writePosition, ByteBuffer byteBuffer) {
        this.writePosition = new AtomicInteger(writePosition);
        this.fileSize = mappedFile.fileSize;
        this.file = mappedFile.file;
        this.startOffset = mappedFile.startOffset + offset;
        this.fileChannel = mappedFile.fileChannel;
        this.mappedByteBuffer = byteBuffer;
    }

    @Override
    public String toString() {
        return this.file.getPath() + "#" + this.writePosition.get();
    }

    /**
     * 写的位置 >= 文件大小
     */
    public boolean isFull() {
        return this.writePosition.get() >= this.fileSize;
    }

    public int getWritePosition() {
        return this.writePosition.get();
    }

    public ByteBuffer getByteBuffer() {
        return this.mappedByteBuffer.slice();
    }

    /**
     * 根据偏移量查找映射文件
     *
     * @link org.apache.rocketmq.store.MappedFile#selectMappedBuffer
     */
    public MappedFile slice(int position, int length) {
        if (position < 0) {
            return null;
        }
        int writePosition = this.writePosition.get();
        if (position + length >= writePosition) {
            // 没什么内容可以读取了
            return null;
        }
        if (length <= 0) {
            // 不使用指定的limit参数
            length = writePosition - position;
        }
        // mappedByteBuffer的position不会变, 所以需要2次slice
        ByteBuffer byteBuffer = this.mappedByteBuffer
                .slice().position(position)
                .slice().limit(length);
        return new MappedFile(this, position, length, byteBuffer);
    }

    public void append(int position, ByteBuffer byteBuffer, int length) throws PutException {
        if (isFull()) {
            log.error("file is full: file={}, writePosition={}, fileSize={}", this.file, this.writePosition.get(), fileSize);
            throw new PutException(PutStatus.UNKNOWN_ERROR);
        }
        this.mappedByteBuffer.slice().put(byteBuffer.array(), position, byteBuffer.limit());
        // 记录写入字节数: 在EOF的时候, mappedByteBuffer的position<fileSize, 此时无法判断是否写满
        this.writePosition.addAndGet(length);
    }

    public void append(ByteBuffer byteBuffer) throws PutException {
        append(0, byteBuffer, byteBuffer.limit());
    }

    public void append(ByteBuffer byteBuffer, int length) throws PutException {
        append(0, byteBuffer, length);
    }

    public void append(int position, ByteBuffer byteBuffer) throws PutException {
        append(position, byteBuffer, byteBuffer.limit());
    }

    public void appendInt(int position, int value) {
        this.mappedByteBuffer.slice().putInt(position, value);
        this.writePosition.addAndGet(4);
    }

    public void appendLong(int position, long value) {
        this.mappedByteBuffer.slice().putLong(position, value);
        this.writePosition.addAndGet(8);
    }
}
