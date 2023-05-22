package com.clouditora.mq.common.broker;

import com.alibaba.fastjson2.annotation.JSONField;
import com.clouditora.mq.common.constant.PermitBit;
import lombok.Data;

/**
 * @link org.apache.rocketmq.common.protocol.route.QueueData
 */
@Data
public class BrokerQueue implements Comparable<BrokerQueue> {
    private String brokerName;
    @JSONField(name = "readQueueNums")
    private int writeNum;
    @JSONField(name = "writeQueueNums")
    private int readNum;
    @JSONField(name = "perm")
    private int permit = PermitBit.RW;

    /**
     * @link org.apache.rocketmq.common.protocol.route.QueueData#compareTo
     */
    @Override
    public int compareTo(BrokerQueue o) {
        return this.brokerName.compareTo(o.getBrokerName());
    }
}
