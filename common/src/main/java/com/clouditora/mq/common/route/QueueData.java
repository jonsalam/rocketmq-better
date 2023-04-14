package com.clouditora.mq.common.route;

import lombok.Data;

/**
 * @link org.apache.rocketmq.common.protocol.route.QueueData
 */
@Data
public class QueueData implements Comparable<QueueData> {
    private String brokerName;
    private int readQueueNums;
    private int writeQueueNums;
    private int perm;
    private int topicSysFlag;

    @Override
    public int compareTo(QueueData o) {
        return this.brokerName.compareTo(o.getBrokerName());
    }
}
