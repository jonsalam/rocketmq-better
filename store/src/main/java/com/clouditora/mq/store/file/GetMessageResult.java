package com.clouditora.mq.store.file;

import com.clouditora.mq.store.enums.GetMessageStatus;
import lombok.Data;
import org.apache.commons.collections4.CollectionUtils;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * @link org.apache.rocketmq.store.GetMessageResult
 */
@Data
public class GetMessageResult {
    private GetMessageStatus status;
    private List<MappedFile> slices;
    private long nextBeginOffset;
    private long minOffset;
    private long maxOffset;

    public GetMessageResult() {
        this.slices = new ArrayList<>();
    }

    public void add(MappedFile file) {
        this.slices.add(file);
    }

    public byte[] covert2bytes() {
        if (CollectionUtils.isEmpty(this.slices)) {
            return new byte[0];
        }
        long size = this.slices.stream().map(MappedFile::getByteBuffer).mapToLong(Buffer::limit).sum();
        ByteBuffer byteBuffer = ByteBuffer.allocate((int) size);
        this.slices.stream().map(MappedFile::getByteBuffer).forEach(byteBuffer::put);
        return byteBuffer.array();
    }
}
