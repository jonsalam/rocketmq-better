package com.clouditora.mq.nameserver.broker;

import io.netty.channel.Channel;
import lombok.Data;

/**
 * @link org.apache.rocketmq.namesrv.routeinfo.BrokerLiveInfo
 */
@Data
public class BrokerAlive {
    private long updateTime = System.currentTimeMillis();
    private String endpoint;
    private Channel channel;

    public boolean isInactive(long timeout) {
        return System.currentTimeMillis() - updateTime > timeout;
    }
}
