package com.clouditora.mq.client.consumer.handler;

import com.clouditora.mq.common.message.MessageEntity;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @link org.apache.rocketmq.client.impl.consumer.ProcessQueue
 */
@Slf4j
public class ConsumerQueue {
    public final static long REBALANCE_LOCK_MAX_LIVE_TIME = Long.parseLong(System.getProperty("rocketmq.client.rebalance.lockMaxLiveTime", "30000"));
    public final static long REBALANCE_LOCK_INTERVAL = Long.parseLong(System.getProperty("rocketmq.client.rebalance.lockInterval", "20000"));
    private final static long PULL_MAX_IDLE_TIME = Long.parseLong(System.getProperty("rocketmq.client.pull.pullMaxIdleTime", "120000"));

    @Setter
    private volatile long lastPullTimestamp = System.currentTimeMillis();
    /**
     * @link org.apache.rocketmq.client.impl.consumer.ProcessQueue#msgCount
     */
    private final AtomicLong messageCount = new AtomicLong();
    /**
     * @link org.apache.rocketmq.client.impl.consumer.ProcessQueue#msgSize
     */
    private final AtomicLong messageSize = new AtomicLong();
    /**
     * offset:
     *
     * @link org.apache.rocketmq.client.impl.consumer.ProcessQueue#msgTreeMap
     */
    private final SortedMap<Long, MessageEntity> messageMap = new TreeMap<>();
    @Getter
    private volatile boolean locked = false;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public long getMessageCount() {
        return this.messageCount.get();
    }

    public long getMessageSize() {
        return this.messageSize.get();
    }

    /**
     * @link org.apache.rocketmq.client.impl.consumer.ProcessQueue#getMaxSpan
     */
    public long getMaxSpan() {
        try {
            this.lock.readLock().lockInterruptibly();
            try {
                if (!this.messageMap.isEmpty()) {
                    return this.messageMap.lastKey() - this.messageMap.firstKey();
                }
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (InterruptedException ignored) {
        }

        return 0;
    }

    /**
     * @link org.apache.rocketmq.client.impl.consumer.ProcessQueue#putMessage
     */
    public boolean putMessages(List<MessageEntity> messages) {
        boolean dispatchToConsume = false;
        try {
            this.lock.writeLock().lockInterruptibly();
            try {
                int validMsgCnt = 0;
                for (MessageEntity message : messages) {
                    MessageEntity old = this.messageMap.put(message.getQueueOffset(), message);
                    if (old == null) {
                        validMsgCnt++;
//                        this.queueOffsetMax = message.getQueueOffset();
                        this.messageSize.addAndGet(message.getBody().length);
                    }
                }
                this.messageCount.addAndGet(validMsgCnt);

//                if (!this.messageMap.isEmpty() && !this.consuming) {
//                    dispatchToConsume = true;
//                    this.consuming = true;
//                }
            } finally {
                this.lock.writeLock().unlock();
            }
        } catch (InterruptedException ignored) {
        }
        return dispatchToConsume;
    }

    public long removeMessage(List<MessageEntity> messages) {
        long now = System.currentTimeMillis();
        try {
            this.lock.writeLock().lockInterruptibly();
//            this.lastConsumeTimestamp = now;
            try {
                if (this.messageMap.isEmpty()) {
                    return -1;
                }
//                long result = this.queueOffsetMax + 1;
                long result = 0;
                for (MessageEntity message : messages) {
                    MessageEntity prev = this.messageMap.remove(message.getQueueOffset());
                    if (prev != null) {
                        this.messageSize.addAndGet(-message.getBody().length);
                        this.messageCount.decrementAndGet();
                    }
                }
                if (!this.messageMap.isEmpty()) {
                    result = this.messageMap.firstKey();
                }
                return result;
            } finally {
                this.lock.writeLock().unlock();
            }
        } catch (InterruptedException ignored) {
        }
        return -1;
    }
}
