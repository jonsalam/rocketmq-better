package com.clouditora.mq.client.consumer.consume;

/**
 * @link org.apache.rocketmq.common.protocol.body.ConsumeMessageDirectlyResult
 */
public class ConsumeResult {
    private boolean order = false;
    private boolean autoCommit = true;
    private ConsumeStatus consumeResult;
    private String remark;
    private long spentTimeMills;
}
