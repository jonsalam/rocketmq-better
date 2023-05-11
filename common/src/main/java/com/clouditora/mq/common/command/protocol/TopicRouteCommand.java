package com.clouditora.mq.common.command.protocol;

import com.alibaba.fastjson2.annotation.JSONField;
import com.clouditora.mq.common.command.CommandHeader;
import com.clouditora.mq.common.route.BrokerEndpoint;
import com.clouditora.mq.common.route.TopicRoute;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.List;

public class TopicRouteCommand {

    /**
     * @link org.apache.rocketmq.common.protocol.header.namesrv.GetRouteInfoRequestHeader
     */
    @Data
    public static class RequestHeader implements CommandHeader {
        private String topic;
    }

    /**
     * @link org.apache.rocketmq.common.protocol.route.TopicRouteData
     */
    @ToString(callSuper = true)
    @EqualsAndHashCode(callSuper = true)
    @Data
    public static class ResponseBody extends TopicRoute {
        @JSONField(name = "brokerDatas")
        private List<BrokerEndpoint> brokers;
    }
}
