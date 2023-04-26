package com.clouditora.mq.broker.client;

import com.clouditora.mq.network.protocol.LanguageCode;
import io.netty.channel.Channel;
import lombok.Data;

/**
 * @link org.apache.rocketmq.broker.client.ClientChannelInfo
 */
@Data
public class ClientChannel {
    private Channel channel;
    private String clientId;
    private LanguageCode language;
    private long updateTime = System.currentTimeMillis();
}
