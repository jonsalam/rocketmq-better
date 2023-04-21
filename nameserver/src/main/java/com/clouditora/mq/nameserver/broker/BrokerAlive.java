package com.clouditora.mq.nameserver.broker;

import com.alibaba.fastjson2.annotation.JSONField;
import io.netty.channel.Channel;
import lombok.Data;

/**
 * @link org.apache.rocketmq.namesrv.routeinfo.BrokerLiveInfo
 */
@Data
public class BrokerAlive {
    private long updateTime = System.currentTimeMillis();
    @JSONField(name = "brokerAddress")
    private String endpoint;
    private Channel channel;
}
