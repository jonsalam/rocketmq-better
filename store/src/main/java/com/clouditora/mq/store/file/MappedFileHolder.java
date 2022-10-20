package com.clouditora.mq.store.file;

import com.clouditora.mq.store.enums.GetStatus;
import lombok.Data;
import org.apache.commons.collections4.CollectionUtils;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

@Data
public class MappedFileHolder {
    private GetStatus status;
    private List<MappedFile> slices;

    public MappedFileHolder() {
        this.slices = new ArrayList<>();
    }

    public void addMappedFile(MappedFile file) {
        slices.add(file);
    }

    public byte[] covert2bytes() {
        if (CollectionUtils.isEmpty(slices)) {
            return new byte[0];
        }
        long size = slices.stream().map(MappedFile::getByteBuffer).mapToLong(Buffer::limit).sum();
        ByteBuffer byteBuffer = ByteBuffer.allocate((int) size);
        slices.stream().map(MappedFile::getByteBuffer).forEach(byteBuffer::put);
        return byteBuffer.array();
    }
}
