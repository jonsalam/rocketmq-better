package com.clouditora.mq.common.topic;

import com.alibaba.fastjson2.annotation.JSONField;
import com.clouditora.mq.common.broker.BrokerEndpoints;
import com.clouditora.mq.common.broker.BrokerQueue;
import lombok.Data;
import org.apache.commons.collections4.CollectionUtils;

import java.util.List;

/**
 * @link org.apache.rocketmq.common.protocol.route.TopicRouteData
 */
@Data
public class TopicRoute {
    @JSONField(name = "brokerDatas")
    private List<BrokerEndpoints> brokers;
    @JSONField(name = "queueDatas")
    private List<BrokerQueue> queues;

    public boolean isEmpty() {
        if (CollectionUtils.isEmpty(this.brokers)) {
            return true;
        }
        return CollectionUtils.isEmpty(this.queues);
    }
}
