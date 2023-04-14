package com.clouditora.mq.client.consumer;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @link org.apache.rocketmq.client.impl.consumer.ProcessQueue
 */
@Slf4j
@Data
public class ProcessQueue {
    public final static long REBALANCE_LOCK_MAX_LIVE_TIME = Long.parseLong(System.getProperty("rocketmq.client.rebalance.lockMaxLiveTime", "30000"));
    public final static long REBALANCE_LOCK_INTERVAL = Long.parseLong(System.getProperty("rocketmq.client.rebalance.lockInterval", "20000"));
    public final static long PULL_MAX_IDLE_TIME = Long.parseLong(System.getProperty("rocketmq.client.pull.pullMaxIdleTime", "120000"));

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock consumeLock = new ReentrantLock();

    private final SortedMap<Long, Object> msgTreeMap = new TreeMap<>();
    private final AtomicLong msgCount = new AtomicLong();
    private final AtomicLong msgSize = new AtomicLong();
    /**
     * A subset of msgTreeMap, will only be used when orderly consume
     */
    private final TreeMap<Long, Object> consumingMsgOrderlyTreeMap = new TreeMap<>();

    private volatile long queueOffsetMax = 0L;
    private volatile boolean dropped = false;
    private volatile long lastPullTimestamp = System.currentTimeMillis();
    private volatile long lastConsumeTimestamp = System.currentTimeMillis();
    private volatile boolean locked = false;
    private volatile long lastLockTimestamp = System.currentTimeMillis();
    private volatile boolean consuming = false;
    private volatile long msgAccCnt = 0;
}
