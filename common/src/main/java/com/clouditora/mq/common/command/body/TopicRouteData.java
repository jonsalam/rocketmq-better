package com.clouditora.mq.common.command.body;

import com.clouditora.mq.common.route.BrokerData;
import com.clouditora.mq.common.route.QueueData;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.List;
import java.util.Map;

/**
 * @link org.apache.rocketmq.common.protocol.route.TopicRouteData
 */
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@Data
public class TopicRouteData extends JsonBodyCommand {
    private String orderTopicConf;
    private List<QueueData> queueDatas;
    private List<BrokerData> brokerDatas;
    /**
     * brokerAddr: Filter Server
     */
    private Map<String, List<String>> filterServerTable;
}
