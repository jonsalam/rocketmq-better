package com.clouditora.mq.store.index;

import com.clouditora.mq.store.file.MappedFile;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@Slf4j
public class IndexFile extends MappedFile {
    public static final int HEADER_SIZE = 40;
    public static final int SLOT_SIZE = 4;
    public static final int INDEX_SIZE = 20;

    private final int maxSlotCount;
    private final int maxItemCount;

    private int slotCount = 0;
    private int itemCount = 0;
    private long beginTimestamp = 0;
    private long endTimestamp = 0;
    private long beginLogOffset = 0;
    private long endLogOffset = 0;

    public IndexFile(String path, int maxSlotCount, int maxItemCount) throws IOException {
        super(path, HEADER_SIZE + (SLOT_SIZE * maxSlotCount) + (INDEX_SIZE * maxItemCount));
        this.maxSlotCount = maxSlotCount;
        this.maxItemCount = maxItemCount;
    }

    @Override
    public boolean isFull() {
        return itemCount >= maxItemCount;
    }

    public void putKey(String key, long logOffset, long storeTimestamp) {
        if (itemCount >= maxItemCount) {
            log.warn("Over index file capacity: index count={}, index max num={}", itemCount, maxItemCount);
            return;
        }

        int keyHash = keyHash(key);
        int slot = keyHash % maxSlotCount;
        int slotPosition = HEADER_SIZE + (SLOT_SIZE * slot);
        int bucketCount = readItemCount(slotPosition);
        // item
        {
            int position = HEADER_SIZE + (SLOT_SIZE * maxSlotCount) + (INDEX_SIZE * (itemCount + 1));
            int timeDiff = calcTimeDiff(storeTimestamp);
            mappedByteBuffer.position(position);
            mappedByteBuffer.putInt(keyHash);
            mappedByteBuffer.putLong(logOffset);
            mappedByteBuffer.putInt(timeDiff);
            mappedByteBuffer.putInt(bucketCount);
        }
        // slot
        {
            mappedByteBuffer.putInt(slotPosition, itemCount + 1);
        }
        // header
        {
            if (itemCount == 0) {
                beginTimestamp = storeTimestamp;
                mappedByteBuffer.putLong(0, beginTimestamp);
            }

            endTimestamp = storeTimestamp;
            mappedByteBuffer.putLong(8, endTimestamp);

            if (itemCount == 0) {
                beginLogOffset = logOffset;
                mappedByteBuffer.putLong(16, beginLogOffset);
            }

            endLogOffset = logOffset;
            mappedByteBuffer.putLong(24, endLogOffset);

            if (bucketCount == 0) {
                // 没有hash冲突
                slotCount += 1;
                mappedByteBuffer.putInt(32, slotCount);
            }

            itemCount += 1;
            mappedByteBuffer.putInt(36, itemCount);
        }
    }

    private int keyHash(String key) {
        return Math.abs(key.hashCode());
    }

    private int calcTimeDiff(long storeTimestamp) {
        if (beginTimestamp <= 0) {
            return 0;
        }
        long diff = (storeTimestamp - beginTimestamp) / 1000;
        if (diff < 0) {
            return 0;
        }
        if (diff > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }
        return (int) diff;
    }

    private int readItemCount(int slotPosition) {
        int itemCount = this.mappedByteBuffer.getInt(slotPosition);
        if (itemCount < 0 || itemCount > maxItemCount) {
            return 0;
        }
        return itemCount;
    }
}
