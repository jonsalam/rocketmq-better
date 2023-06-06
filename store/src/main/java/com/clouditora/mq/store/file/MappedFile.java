package com.clouditora.mq.store.file;

import com.clouditora.mq.store.enums.PutStatus;
import com.clouditora.mq.store.exception.PutException;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * mappedByteBuffer的position不改变, 手动额外维护读写position
 *
 * @link org.apache.rocketmq.store.MappedFile
 */
@Slf4j
public class MappedFile extends AbstractMappedFile {
    /**
     * 如果文件没有写满, 值与mappedByteBuffer的position相同
     * 如果文件已经写满, 值与fileSize相同
     *
     * @link org.apache.rocketmq.store.MappedFile#wrotePosition
     */
    protected final AtomicInteger writePosition;

    public MappedFile(String path, int fileSize) throws IOException {
        super(new File(path), fileSize);
        this.writePosition = new AtomicInteger(0);
    }

    public MappedFile(MappedFile file, int writePosition, int length, MappedByteBuffer byteBuffer) {
        super(file, writePosition + length, byteBuffer);
        this.writePosition = new AtomicInteger(writePosition);
    }

    /**
     * @link org.apache.rocketmq.store.MappedFile#getWrotePosition
     */
    @Override
    public int getWritePosition() {
        return this.writePosition.get();
    }

    @Override
    public void setWritePosition(int position) {
        this.writePosition.set(position);
    }

    /**
     * @link org.apache.rocketmq.store.MappedFile#getMappedByteBuffer
     */
    @Override
    public MappedByteBuffer getByteBuffer() {
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
        // mappedByteBuffer的position一直为0, 所以需要2次slice
        MappedByteBuffer byteBuffer = this.mappedByteBuffer.slice().position(position).slice().limit(length);
        return new MappedFile(this, position, length, byteBuffer);
    }

    public MappedFile slice(int position) {
        return slice(position, 0);
    }

    /**
     * @param position    从指定位置开始写数据
     * @param bytes       数据
     * @param byteLength  数据长度
     * @param writeLength 指定长度: 用于写指针
     * @link org.apache.rocketmq.store.MappedFile#appendMessage(byte[], int, int)
     */
    public void append(int position, byte[] bytes, int byteLength, int writeLength) throws PutException {
        if (isFull()) {
            log.error("file is full: file={}, writePosition={}, fileSize={}", this.file, this.writePosition.get(), fileSize);
            throw new PutException(PutStatus.UNKNOWN_ERROR);
        }
        this.mappedByteBuffer.slice().put(bytes, position, byteLength);
        // 记录写入字节数: 在EOF的时候, mappedByteBuffer的position<fileSize, 此时无法判断是否写满
        this.writePosition.addAndGet(writeLength);
    }

    public void append(byte[] bytes, int length) throws PutException {
        append(0, bytes, length, length);
    }

    public void append(byte[] bytes) throws PutException {
        append(0, bytes, bytes.length, bytes.length);
    }

    public void append(ByteBuffer byteBuffer, int length) throws PutException {
        append(0, byteBuffer.array(), byteBuffer.limit(), length);
    }

    public void append(ByteBuffer byteBuffer) throws PutException {
        append(0, byteBuffer.array(), byteBuffer.limit(), byteBuffer.limit());
    }
}
