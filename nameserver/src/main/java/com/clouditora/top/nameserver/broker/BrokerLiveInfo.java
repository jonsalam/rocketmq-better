package com.clouditora.top.nameserver.broker;

import com.clouditora.mq.common.route.DataVersion;
import io.netty.channel.Channel;
import lombok.Data;

/**
 * @link org.apache.rocketmq.namesrv.routeinfo.BrokerLiveInfo
 */
@Data
public class BrokerLiveInfo {
    private long lastUpdateTimestamp;
    private DataVersion dataVersion;
    private String brokerAddress;
    private Channel channel;
    private String haServerAddr;
}
