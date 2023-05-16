package com.clouditora.mq.common.network.command;

import com.clouditora.mq.common.network.CommandHeader;
import com.clouditora.mq.common.network.CommandJsonBody;
import com.clouditora.mq.common.topic.TopicRoute;
import lombok.Data;

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
    public static class ResponseBody extends TopicRoute implements CommandJsonBody {
    }
}
