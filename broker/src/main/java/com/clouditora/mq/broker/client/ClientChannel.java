package com.clouditora.mq.broker.client;

import com.clouditora.mq.network.protocol.LanguageCode;
import io.netty.channel.Channel;
import lombok.Data;

/**
 * @link org.apache.rocketmq.broker.client.ClientChannelInfo
 */
@Data
public class ClientChannel {
    private String clientId;
    private Channel channel;
    private LanguageCode language;
    private long updateTime = System.currentTimeMillis();
}
