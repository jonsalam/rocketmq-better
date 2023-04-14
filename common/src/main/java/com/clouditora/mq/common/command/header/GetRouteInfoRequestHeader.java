package com.clouditora.mq.common.command.header;

import lombok.Data;

/**
 * @link org.apache.rocketmq.common.protocol.header.namesrv.GetRouteInfoRequestHeader
 */
@Data
public class GetRouteInfoRequestHeader implements CommandHeader {
    private String topic;
    private Boolean acceptStandardJsonOnly;
}
