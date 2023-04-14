package com.clouditora.mq.common.route;

import lombok.Data;

import java.util.HashMap;

/**
 * @link org.apache.rocketmq.common.protocol.route.BrokerData
 */
@Data
public class BrokerData implements Comparable<BrokerData> {
    private String cluster;
    private String brokerName;
    /**
     * brokerId: broker address
     */
    private HashMap<Long, String> brokerAddrs;

    @Override
    public int compareTo(BrokerData o) {
        return this.brokerName.compareTo(o.getBrokerName());
    }
}
