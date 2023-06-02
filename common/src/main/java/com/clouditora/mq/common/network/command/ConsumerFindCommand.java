package com.clouditora.mq.common.network.command;

import com.alibaba.fastjson2.annotation.JSONField;
import com.clouditora.mq.common.network.CommandHeader;
import com.clouditora.mq.common.network.CommandJsonBody;
import lombok.Data;

import java.util.List;

public class ConsumerFindCommand {
    /**
     * @link org.apache.rocketmq.common.protocol.header.GetConsumerListByGroupRequestHeader
     */
    @Data
    public static class RequestHeader implements CommandHeader {
        @JSONField(name = "consumerGroup")
        private String group;
    }

    /**
     * @link org.apache.rocketmq.common.protocol.header.GetConsumerListByGroupResponseBody
     */
    @Data
    public static class ResponseBody implements CommandJsonBody {
        @JSONField(name = "consumerIdList")
        private List<String> consumerIds;
    }
}
